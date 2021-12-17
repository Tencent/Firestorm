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

package com.tencent.rss.client.impl;

import com.tencent.rss.common.util.RssUtils;
import com.tencent.rss.storage.HdfsTestBase;

import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.roaringbitmap.longlong.Roaring64NavigableMap;
import com.tencent.rss.client.util.ClientUtils;

public class CheckBlockIdsBenchmark extends HdfsTestBase {
  private static final String EXPECTED_EXCEPTION_MESSAGE = "Exception should be thrown";
  private static AtomicLong ATOMIC_LONG = new AtomicLong(0);
  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  public void performanceTest() throws Exception {
    checkBlockIds(1000L);
    checkBlockIds(10000L);
    checkBlockIds(100000L);
  }

  void checkBlockIds(long num) {
    Roaring64NavigableMap blockIds = Roaring64NavigableMap.bitmapOf();
    for (long i = 0; i < num; i++) {
      blockIds.addLong(ClientUtils.getBlockId(i, 0, ATOMIC_LONG.getAndIncrement()));
      blockIds.addLong(ClientUtils.getBlockId(i, 1, ATOMIC_LONG.getAndIncrement()));
      blockIds.addLong(ClientUtils.getBlockId(i, 2, ATOMIC_LONG.getAndIncrement()));
    }

    long start = System.currentTimeMillis();
    Roaring64NavigableMap processedBlockIds = Roaring64NavigableMap.bitmapOf();
    Iterator<Long> iter = blockIds.iterator();
    while(iter.hasNext()) {
      long blockId = iter.next();
      processedBlockIds.addLong(blockId);
    }
    long cost = System.currentTimeMillis() - start;
    System.out.println("W/O checkBlockIds (" + num + " partitions): " + cost + " ms");

    Roaring64NavigableMap pendingBlockIds;
    try {
      pendingBlockIds = RssUtils.deserializeBitMap(RssUtils.serializeBitMap(blockIds));
      pendingBlockIds.and(blockIds);
    } catch (IOException ioe) {
      throw new RuntimeException("Can't create pending blockIds.", ioe);
    }
    iter = blockIds.iterator();
    while(iter.hasNext()) {
      long blockId = iter.next();
      pendingBlockIds.removeLong(blockId);
    }
    cost = System.currentTimeMillis() - start;
    System.out.println("W/  checkBlockIds (" + num + " partitions): " + cost + " ms");
    System.out.println("----------------------------------------------------------");
  }
}
