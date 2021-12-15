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

import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import com.tencent.rss.common.ShuffleDataResult;
import com.tencent.rss.server.ShuffleFlushManager;
import com.tencent.rss.server.ShuffleServerConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class FSShuffleBufferManager extends ShuffleBufferManager {

  private static final Logger LOG = LoggerFactory.getLogger(ShuffleBufferManager.class);
  protected long spillThreshold;

  public FSShuffleBufferManager(ShuffleServerConf conf, ShuffleFlushManager shuffleFlushManager) {
    super(conf, shuffleFlushManager);
    this.spillThreshold = conf.getSizeAsBytes(ShuffleServerConf.SERVER_BUFFER_SPILL_THRESHOLD);
    this.bufferSize = conf.getSizeAsBytes(ShuffleServerConf.SERVER_PARTITION_BUFFER_SIZE);
  }

  @Override
  public ShuffleDataResult getShuffleData(
      String appId, int shuffleId, int partitionId, long blockId, int readBufferSize) {
    throw new RuntimeException("Doesn't support getShuffleData with FSShuffleBufferManager");
  }

  @Override
  void flushIfNecessary(String appId, int shuffleId, ShuffleBuffer buffer, Range<Integer> range) {
    if (shouldFlush()) {
      LOG.info("Buffer manager is full with usedMemory[" + usedMemory.get() + "], preAllocation["
          + preAllocatedSize.get() + "], inFlush[" + inFlushSize.get() + "]");
      flush(Maps.newHashMap());
    } else if (buffer.isFull()) {
      LOG.info("Single buff is full with " + buffer.getSize() + " bytes");
      flushBuffer(buffer, appId, shuffleId, range.lowerEndpoint(), range.upperEndpoint());
    }
  }

  @Override
  public synchronized void commitShuffleTask(String appId, int shuffleId) {
    RangeMap<Integer, ShuffleBuffer> buffers = bufferPool.get(appId).get(shuffleId);
    for (Map.Entry<Range<Integer>, ShuffleBuffer> entry : buffers.asMapOfRanges().entrySet()) {
      ShuffleBuffer buffer = entry.getValue();
      Range<Integer> range = entry.getKey();
      flushBuffer(buffer, appId, shuffleId, range.lowerEndpoint(), range.upperEndpoint());
    }
  }

  // if data size in buffer > spillThreshold, do the flush
  private boolean shouldFlush() {
    return usedMemory.get() - preAllocatedSize.get() - inFlushSize.get() > spillThreshold;
  }
}
