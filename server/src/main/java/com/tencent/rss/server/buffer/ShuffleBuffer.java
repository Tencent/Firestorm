/*
 * Tencent is pleased to support the open source community by making
 * Firestorm-Spark remote shuffle server available. 
 *
 * Copyright (C) 2021 THL A29 Limited, a Tencent company.  All rights reserved. 
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * https://opensource.org/licenses/Apache-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.tencent.rss.server.buffer;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.tencent.rss.common.BufferSegment;
import com.tencent.rss.common.ShuffleDataResult;
import com.tencent.rss.common.ShufflePartitionedBlock;
import com.tencent.rss.common.ShufflePartitionedData;
import com.tencent.rss.common.util.Constants;
import com.tencent.rss.server.ShuffleDataFlushEvent;
import com.tencent.rss.server.ShuffleFlushManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

public class ShuffleBuffer {

  private static final Logger LOG = LoggerFactory.getLogger(ShuffleBuffer.class);

  private final long capacity;
  private long size;
  // blocks will be added to inFlushBlockMap as <eventId, blocks> pair
  // it will be removed after flush to storage
  // the strategy ensure that shuffle is in memory or storage
  private List<ShufflePartitionedBlock> blocks;
  private Map<Long, List<ShufflePartitionedBlock>> inFlushBlockMap;
  private static final long INVALID_EVENT_ID = -1;

  public ShuffleBuffer(long capacity) {
    this.capacity = capacity;
    this.size = 0;
    this.blocks = new LinkedList<>();
    this.inFlushBlockMap = Maps.newConcurrentMap();
  }

  public int append(ShufflePartitionedData data) {
    int mSize = 0;

    synchronized (this) {
      for (ShufflePartitionedBlock block : data.getBlockList()) {
        blocks.add(block);
        mSize += block.getSize();
        size += mSize;
      }
    }

    return mSize;
  }

  public synchronized ShuffleDataFlushEvent toFlushEvent(
      String appId,
      int shuffleId,
      int startPartition,
      int endPartition,
      Map<String, Map<Integer, AtomicLong>> shuffleSizeMap,
      Supplier<Boolean> isValid) {
    if (blocks.isEmpty()) {
      return null;
    }
    // buffer will be cleared, and new list must be created for async flush
    List<ShufflePartitionedBlock> spBlocks = new LinkedList<>(blocks);
    long eventId = ShuffleFlushManager.ATOMIC_EVENT_ID.getAndIncrement();
    final ShuffleDataFlushEvent event = new ShuffleDataFlushEvent(
        eventId,
        appId,
        shuffleId,
        startPartition,
        endPartition,
        size,
        spBlocks,
        isValid,
        this);
    inFlushBlockMap.put(eventId, spBlocks);
    blocks.clear();
    updateShuffleSize(shuffleSizeMap, appId, shuffleId, size);
    size = 0;
    return event;
  }

  // update metadata of shuffleId -> size
  private void updateShuffleSize(
      Map<String, Map<Integer, AtomicLong>> shuffleSizeMap,
      String appId,
      Integer shuffleId,
      long flushSize) {
    Map<Integer, AtomicLong> shuffleIdToSize = shuffleSizeMap.get(appId);
    if (shuffleIdToSize != null) {
      AtomicLong atomicLong = shuffleIdToSize.get(shuffleId);
      if (atomicLong != null) {
        atomicLong.addAndGet(-flushSize);
      }
    }
  }

  public List<ShufflePartitionedBlock> getBlocks() {
    return blocks;
  }

  public long getSize() {
    return size;
  }

  public boolean isFull() {
    return size > capacity;
  }

  public synchronized void clearInFlushBuffer(long eventId) {
    inFlushBlockMap.remove(eventId);
  }

  @VisibleForTesting
  public Map<Long, List<ShufflePartitionedBlock>> getInFlushBlockMap() {
    return inFlushBlockMap;
  }

  // 1. generate buffer segments and other info: if blockId exist, start with which eventId
  // 2. according to info from step 1, generate data
  public synchronized ShuffleDataResult getShuffleData(
      long lastBlockId, int readBufferSize) {
    try {
      List<BufferSegment> bufferSegments = Lists.newArrayList();
      ActualReadInfo actualReadInfo = updateBufferSegments(
          lastBlockId, readBufferSize, bufferSegments);
      if (actualReadInfo.actualReadSize > 0) {
        byte[] data = new byte[actualReadInfo.actualReadSize];
        updateShuffleData(actualReadInfo.lastBlockId, readBufferSize,
            data, actualReadInfo.fromEventId);
        return new ShuffleDataResult(data, bufferSegments);
      }
    } catch (Exception e) {
      LOG.error("Exception happened when getShuffleData in buffer", e);
    }
    return new ShuffleDataResult();
  }

  // here is the rule to read data in memory:
  // 1. read from inFlushBlockMap order by eventId asc, then from blocks
  // 2. if can't find lastBlockId, means related data may be flushed to storage, repeat step 1
  private ActualReadInfo updateBufferSegments(
      long lastBlockId,
      long readBufferSize,
      List<BufferSegment> bufferSegments) throws Exception {
    long fromEventId = INVALID_EVENT_ID;
    long currentBlockId = lastBlockId;
    List<Long> sortedEventId = sortFlushingEventId();
    int offset = 0;
    int eventIndex = 0;
    long updatedLastBlockId = lastBlockId;
    boolean hasBlockId = false;
    // read from inFlushBlockMap first
    if (inFlushBlockMap.size() > 0) {
      for (Long eventId : sortedEventId) {
        ListReadInfo listReadInfo = processCachedBlocks(offset, inFlushBlockMap.get(eventId),
            currentBlockId, readBufferSize, bufferSegments);
        offset = listReadInfo.offset;
        // try set which eventId is the start point
        if (fromEventId == INVALID_EVENT_ID) {
          if (offset > 0) {
            // offset > 0, start from current event
            fromEventId = eventId;
            // read from 1st block with following inFlushBlock/blocks
            currentBlockId = Constants.INVALID_BLOCK_ID;
          } else if (listReadInfo.hasBlockId) {
            // offset = 0, but find the blockId, start from next event if exist
            if (eventIndex < sortedEventId.size() - 1) {
              fromEventId = sortedEventId.get(eventIndex + 1);
            }
            hasBlockId = true;
            // read from 1st block with following inFlushBlock/blocks
            currentBlockId = Constants.INVALID_BLOCK_ID;
            updatedLastBlockId = Constants.INVALID_BLOCK_ID;
          }
        }
        if (offset >= readBufferSize) {
          break;
        }
        eventIndex++;
      }
    }
    if (blocks.size() > 0 && offset < readBufferSize) {
      ListReadInfo listReadInfo = processCachedBlocks(
          offset, blocks, currentBlockId, readBufferSize, bufferSegments);
      offset = listReadInfo.offset;
      hasBlockId = listReadInfo.hasBlockId;
    }
    if (lastBlockId != Constants.INVALID_BLOCK_ID && offset == 0 && !hasBlockId) {
      // can't find lastBlockId, it should be flushed
      // try read with blockId = Constants.INVALID_BLOCK_ID
      return updateBufferSegments(Constants.INVALID_BLOCK_ID, readBufferSize, bufferSegments);
    } else {
      return new ActualReadInfo(offset, fromEventId, updatedLastBlockId);
    }
  }

  private void updateShuffleData(
      long lastBlockId,
      long readBufferSize,
      byte[] data,
      long fromEventId) throws Exception {
    List<Long> sortedEventId = sortFlushingEventId();
    long currentBlockId = lastBlockId;
    int offset = 0;
    if (fromEventId != INVALID_EVENT_ID) {
      // read data start with inFlushBlock
      for (Long eventId : sortedEventId) {
        if (eventId >= fromEventId) {
          ListReadInfo listReadInfo = processCachedBlocks(offset, inFlushBlockMap.get(eventId),
              currentBlockId, readBufferSize, data);
          offset = listReadInfo.offset;
          // from next flush/cache buffer, read from 1st block
          currentBlockId = Constants.INVALID_BLOCK_ID;
          if (offset >= readBufferSize) {
            break;
          }
        }
      }
    }
    if (blocks.size() > 0 && offset < readBufferSize) {
      processCachedBlocks(offset, blocks, currentBlockId, readBufferSize, data);
    }
  }

  private List<Long> sortFlushingEventId() {
    List<Long> eventIdList = Lists.newArrayList(inFlushBlockMap.keySet());
    eventIdList.sort((id1, id2) -> {
      if (id1 > id2) {
        return 1;
      }
      return -1;
    });
    return eventIdList;
  }

  private ListReadInfo processCachedBlocks(
      int offset,
      List<ShufflePartitionedBlock> cachedBlocks,
      long lastBlockId,
      long readBufferSize,
      Object parameter) throws Exception {
    int currentOffset = offset;
    Callable<Integer> callable = null;
    if (lastBlockId == Constants.INVALID_BLOCK_ID) {
      // read from first block
      for (ShufflePartitionedBlock block : cachedBlocks) {
        // do action
        if (parameter instanceof byte[]) {
          callable = new CopyShuffleDataCall((byte[]) parameter, block, currentOffset);
        } else if (parameter instanceof List) {
          callable = new UpdateSegmentCall((List<BufferSegment>) parameter, block, currentOffset);
        }
        // update offset
        currentOffset = callable.call();
        if (currentOffset >= readBufferSize) {
          break;
        }
      }
      return new ListReadInfo(currentOffset, false);
    } else {
      // find lastBlockId, then read from next block
      boolean foundBlockId = false;
      for (ShufflePartitionedBlock block : cachedBlocks) {
        if (!foundBlockId) {
          if (block.getBlockId() == lastBlockId) {
            foundBlockId = true;
          }
          continue;
        }
        if (parameter instanceof byte[]) {
          callable = new CopyShuffleDataCall((byte[]) parameter, block, currentOffset);
        } else if (parameter instanceof List) {
          callable = new UpdateSegmentCall((List<BufferSegment>) parameter, block, currentOffset);
        }
        currentOffset = callable.call();
        if (currentOffset >= readBufferSize) {
          break;
        }
      }
      return new ListReadInfo(currentOffset, foundBlockId);
    }
  }

  class CopyShuffleDataCall implements Callable<Integer> {
    final ShufflePartitionedBlock block;
    final byte[] data;
    final int offset;

    CopyShuffleDataCall(byte[] data, ShufflePartitionedBlock block, int offset) {
      this.block = block;
      this.data = data;
      this.offset = offset;
    }

    @Override
    public Integer call() throws Exception {
      try {
        System.arraycopy(block.getData(), 0, data, offset, block.getLength());
      } catch (Exception e) {
        LOG.error("Unexpect exception for System.arraycopy, length["
            + block.getLength() + "], offset["
            + offset + "], dataLength[" + data.length + "]", e);
        throw e;
      }
      return offset + block.getLength();
    }
  }

  class UpdateSegmentCall implements Callable<Integer> {
    final List<BufferSegment> bufferSegments;
    final ShufflePartitionedBlock block;
    final int offset;

    UpdateSegmentCall(List<BufferSegment> bufferSegments,
        ShufflePartitionedBlock block, int offset) {
      this.block = block;
      this.offset = offset;
      this.bufferSegments = bufferSegments;
    }

    @Override
    public Integer call() throws Exception {
      bufferSegments.add(new BufferSegment(block.getBlockId(), offset, block.getLength(),
          block.getUncompressLength(), block.getCrc(), block.getTaskAttemptId()));
      return offset + block.getLength();
    }
  }

  // the class has info for shuffle data reading
  class ActualReadInfo {
    int actualReadSize;
    long fromEventId;
    long lastBlockId;

    ActualReadInfo(int actualReadSize, long fromEventId, long lastBlockId) {
      this.actualReadSize = actualReadSize;
      this.fromEventId = fromEventId;
      this.lastBlockId = lastBlockId;
    }
  }

  class ListReadInfo {
    int offset;
    boolean hasBlockId;

    ListReadInfo(int offset, boolean hasBlockId) {
      this.offset = offset;
      this.hasBlockId = hasBlockId;
    }
  }
}
