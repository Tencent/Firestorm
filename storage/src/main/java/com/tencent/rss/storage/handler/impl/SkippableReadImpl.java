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

package com.tencent.rss.storage.handler.impl;

import com.google.common.collect.Lists;
import com.tencent.rss.common.ShuffleDataResult;
import com.tencent.rss.common.ShuffleDataSegment;
import com.tencent.rss.common.ShuffleIndexResult;
import com.tencent.rss.common.util.RssUtils;
import org.roaringbitmap.longlong.Roaring64NavigableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public abstract class SkippableReadImpl {
  private static final Logger LOG = LoggerFactory.getLogger(SkippableReadImpl.class);

  protected String appId;
  protected int shuffleId;
  protected int partitionId;
  protected final int readBufferSize;

  protected final List<ShuffleDataSegment> shuffleDataSegments = Lists.newArrayList();
  protected int segmentIndex = 0;

  protected Roaring64NavigableMap expectBlockIds;
  protected Roaring64NavigableMap processBlockIds;

  public SkippableReadImpl(
    String appId,
    int shuffleId,
    int partitionId,
    int readBufferSize,
    Roaring64NavigableMap expectBlockIds,
    Roaring64NavigableMap processBlockIds ) {
    this.appId = appId;
    this.shuffleId = shuffleId;
    this.partitionId = partitionId;
    this.readBufferSize = readBufferSize;
    this.expectBlockIds = expectBlockIds;
    this.processBlockIds = processBlockIds;
  }

  protected abstract ShuffleIndexResult readShuffleIndexImpl();

  protected abstract ShuffleDataResult readShuffleDataImpl(ShuffleDataSegment segment);

  public ShuffleDataResult readShuffleData() {
    if (shuffleDataSegments.isEmpty()) {
      ShuffleIndexResult shuffleIndexResult = readShuffleIndexImpl();
      if (shuffleIndexResult == null || shuffleIndexResult.isEmpty()) {
        return null;
      }

      // segments should skip unexpected and processed blocks
      List<ShuffleDataSegment> unFilteredShuffleDataSegments =
        RssUtils.transIndexDataToSegments(shuffleIndexResult, readBufferSize);

      unFilteredShuffleDataSegments.forEach(segment -> {
          Roaring64NavigableMap blocksOfSegment = Roaring64NavigableMap.bitmapOf();
          segment.getBufferSegments().forEach(block -> blocksOfSegment.addLong(block.getBlockId()));
        // skip unexpected blockIds
        blocksOfSegment.and(expectBlockIds);
          if (blocksOfSegment.getLongCardinality() > 0) {
            // skip processed blockIds
            blocksOfSegment.or(processBlockIds);
            blocksOfSegment.xor(processBlockIds);
            if (blocksOfSegment.getLongCardinality() > 0) {
              shuffleDataSegments.add(segment);
            }
          }
        }
      );
    }

    if (segmentIndex >= shuffleDataSegments.size()) {
      return null;
    }

    ShuffleDataResult sdr = readShuffleDataImpl(shuffleDataSegments.get(segmentIndex));
    segmentIndex ++;
    return sdr;
  }
}
