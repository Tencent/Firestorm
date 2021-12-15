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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;
import com.tencent.rss.common.ShuffleDataResult;
import com.tencent.rss.server.ShuffleFlushManager;
import com.tencent.rss.server.ShuffleServerConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

public class MemoryFSShuffleBufferManager extends ShuffleBufferManager {

  private static final Logger LOG = LoggerFactory.getLogger(MemoryFSShuffleBufferManager.class);
  private static final String KEY_SPLIT_CHAR = "~";
  private long highWaterMark;
  private long lowWaterMark;

  public MemoryFSShuffleBufferManager(
      ShuffleServerConf conf, ShuffleFlushManager shuffleFlushManager) {
    super(conf, shuffleFlushManager);
    this.highWaterMark = conf.getLong(ShuffleServerConf.SERVER_MEMORY_SHUFFLE_HIGHWATERMARK);
    this.lowWaterMark = conf.getLong(ShuffleServerConf.SERVER_MEMORY_SHUFFLE_LOWWATERMARK);
  }

  @Override
  public ShuffleDataResult getShuffleData(
      String appId, int shuffleId, int partitionId, long blockId,
      int readBufferSize) {
    Map.Entry<Range<Integer>, ShuffleBuffer> entry = getShuffleBufferEntry(
      appId, shuffleId, partitionId);
    if (entry == null) {
      return null;
    }

    ShuffleBuffer buffer = entry.getValue();
    if (buffer == null) {
      return null;
    }
    return buffer.getShuffleData(blockId, readBufferSize);
  }

  @Override
  void flushIfNecessary(String appId, int shuffleId, ShuffleBuffer buffer, Range<Integer> range) {
    // if data size in buffer > highWaterMark, do the flush
    if (usedMemory.get() - preAllocatedSize.get() - inFlushSize.get() > highWaterMark) {
      LOG.info("Start to flush with usedMemory[{}], preAllocatedSize[{}], inFlushSize[{}]",
          usedMemory.get(), preAllocatedSize.get(), inFlushSize.get());
      Map<String, Set<Integer>> pickedShuffle = pickFlushedShuffle();
      flush(pickedShuffle);
    }
  }

  @Override
  public void commitShuffleTask(String appId, int shuffleId) {
  }

  // sort for shuffle according to data size, then pick properly data which will be flushed
  private Map<String, Set<Integer>> pickFlushedShuffle() {
    // create list for sort
    List<Map.Entry<String, AtomicLong>> sizeList = generateSizeList();
    sizeList.sort((entry1, entry2) -> {
      if (entry1.getValue().get() > entry2.getValue().get()) {
        return -1;
      }
      return 1;
    });
    Map<String, Set<Integer>> pickedShuffle = Maps.newHashMap();
    // The algorithm here is to flush data size > highWaterMark - lowWaterMark
    // the remain data in buffer maybe more than lowWaterMark
    // because shuffle server is still receiving data, but it should be ok
    long expectedFlushSize = highWaterMark - lowWaterMark;
    long pickedFlushSize = 0L;
    for (Map.Entry<String, AtomicLong> entry : sizeList) {
      long size = entry.getValue().get();
      pickedFlushSize += size;
      String appIdShuffleIdKey = entry.getKey();
      addPickedShuffle(appIdShuffleIdKey, pickedShuffle);
      if (pickedFlushSize > expectedFlushSize) {
        LOG.info("Finish flush pick with {} bytes", pickedFlushSize);
        break;
      }
    }
    return pickedShuffle;
  }

  private List<Map.Entry<String, AtomicLong>> generateSizeList() {
    Map<String, AtomicLong> sizeMap = Maps.newHashMap();
    for (Map.Entry<String, Map<Integer, AtomicLong>> appEntry : shuffleSizeMap.entrySet()) {
      String appId = appEntry.getKey();
      for (Map.Entry<Integer, AtomicLong> shuffleEntry : appEntry.getValue().entrySet()) {
        Integer shuffleId = shuffleEntry.getKey();
        sizeMap.put(appId + KEY_SPLIT_CHAR + shuffleId, shuffleEntry.getValue());
      }
    }
    return Lists.newArrayList(sizeMap.entrySet());
  }

  private void addPickedShuffle(String key, Map<String, Set<Integer>> pickedShuffle) {
    String[] splits = key.split(KEY_SPLIT_CHAR);
    String appId = splits[0];
    Integer shuffleId = Integer.parseInt(splits[1]);
    pickedShuffle.putIfAbsent(appId, Sets.newHashSet());
    Set<Integer> shuffleIdSet = pickedShuffle.get(appId);
    shuffleIdSet.add(shuffleId);
  }
}
