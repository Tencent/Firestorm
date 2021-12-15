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

import com.google.common.collect.RangeMap;
import com.tencent.rss.common.ShuffleDataResult;
import com.tencent.rss.common.ShufflePartitionedData;
import com.tencent.rss.common.util.Constants;
import com.tencent.rss.server.ShuffleFlushManager;
import com.tencent.rss.server.ShuffleServerConf;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;

public class MemoryFSShuffleBufferManagerTest extends BufferTestBase {

  private ShuffleBufferManager shuffleBufferManager;
  private ShuffleFlushManager mockShuffleFlushManager;

  @Before
  public void setUp() {
    ShuffleServerConf conf = new ShuffleServerConf();
    conf.set(ShuffleServerConf.SERVER_BUFFER_CAPACITY, 500L);
    conf.set(ShuffleServerConf.SERVER_MEMORY_SHUFFLE_LOWWATERMARK, 100L);
    conf.set(ShuffleServerConf.SERVER_MEMORY_SHUFFLE_HIGHWATERMARK, 400L);
    mockShuffleFlushManager = mock(ShuffleFlushManager.class);
    shuffleBufferManager = new MemoryFSShuffleBufferManager(conf, mockShuffleFlushManager);
  }

  @Test
  public void getShuffleDataTest() {
    String appId = "getShuffleDataTest";
    shuffleBufferManager.registerBuffer(appId, 1, 0, 1);
    shuffleBufferManager.registerBuffer(appId, 2, 0, 1);
    shuffleBufferManager.registerBuffer(appId, 3, 0, 1);
    shuffleBufferManager.registerBuffer(appId, 4, 0, 1);
    ShufflePartitionedData spd1 = createData(0, 68);
    ShufflePartitionedData spd2 = createData(0, 68);
    ShufflePartitionedData spd3 = createData(0, 68);
    ShufflePartitionedData spd4 = createData(0, 68);
    shuffleBufferManager.cacheShuffleData(appId, 1, false, spd1);
    shuffleBufferManager.cacheShuffleData(appId, 2, false, spd2);
    shuffleBufferManager.cacheShuffleData(appId, 2, false, spd3);
    shuffleBufferManager.cacheShuffleData(appId, 3, false, spd4);
    // validate buffer, no flush happened
    Map<String, Map<Integer, RangeMap<Integer, ShuffleBuffer>>> bufferPool =
        shuffleBufferManager.getBufferPool();
    assertEquals(100, bufferPool.get(appId).get(1).get(0).getSize());
    assertEquals(200, bufferPool.get(appId).get(2).get(0).getSize());
    assertEquals(100, bufferPool.get(appId).get(3).get(0).getSize());
    // validate get shuffle data
    ShuffleDataResult sdr = shuffleBufferManager.getShuffleData(
        appId, 2, 0, Constants.INVALID_BLOCK_ID, 60);
    assertArrayEquals(spd2.getBlockList()[0].getData(), sdr.getData());
    long lastBlockId = spd2.getBlockList()[0].getBlockId();
    sdr = shuffleBufferManager.getShuffleData(
        appId, 2, 0, lastBlockId, 100);
    assertArrayEquals(spd3.getBlockList()[0].getData(), sdr.getData());
    // flush happen
    ShufflePartitionedData spd5 = createData(0, 10);
    shuffleBufferManager.cacheShuffleData(appId, 4, false, spd5);
    // according to flush strategy, some buffers should be moved to inFlushMap
    assertEquals(0, bufferPool.get(appId).get(1).get(0).getBlocks().size());
    assertEquals(1, bufferPool.get(appId).get(1).get(0).getInFlushBlockMap().size());
    assertEquals(0, bufferPool.get(appId).get(2).get(0).getBlocks().size());
    assertEquals(1, bufferPool.get(appId).get(2).get(0).getInFlushBlockMap().size());
    assertEquals(0, bufferPool.get(appId).get(3).get(0).getBlocks().size());
    assertEquals(1, bufferPool.get(appId).get(3).get(0).getInFlushBlockMap().size());
    // keep buffer whose size < low water mark
    assertEquals(1, bufferPool.get(appId).get(4).get(0).getBlocks().size());
    // data in flush buffer now, it also can be got before flush finish
    sdr = shuffleBufferManager.getShuffleData(
        appId, 2, 0, Constants.INVALID_BLOCK_ID, 60);
    assertArrayEquals(spd2.getBlockList()[0].getData(), sdr.getData());
    lastBlockId = spd2.getBlockList()[0].getBlockId();
    sdr = shuffleBufferManager.getShuffleData(
        appId, 2, 0, lastBlockId, 100);
    assertArrayEquals(spd3.getBlockList()[0].getData(), sdr.getData());
    // cache data again, it should cause flush
    spd1 = createData(0, 10);
    shuffleBufferManager.cacheShuffleData(appId, 1, false, spd1);
    assertEquals(1, bufferPool.get(appId).get(1).get(0).getBlocks().size());
    // finish flush
    bufferPool.get(appId).get(1).get(0).getInFlushBlockMap().clear();
    bufferPool.get(appId).get(2).get(0).getInFlushBlockMap().clear();
    bufferPool.get(appId).get(3).get(0).getInFlushBlockMap().clear();
    // empty data return
    sdr = shuffleBufferManager.getShuffleData(
        appId, 2, 0, Constants.INVALID_BLOCK_ID, 60);
    assertEquals(0, sdr.getData().length);
    lastBlockId = spd2.getBlockList()[0].getBlockId();
    sdr = shuffleBufferManager.getShuffleData(
        appId, 2, 0, lastBlockId, 100);
    assertEquals(0, sdr.getData().length);
  }

  @Test
  public void shuffleIdToSizeTest() {
    String appId1 = "shuffleIdToSizeTest1";
    String appId2 = "shuffleIdToSizeTest2";
    shuffleBufferManager.registerBuffer(appId1, 1, 0, 0);
    shuffleBufferManager.registerBuffer(appId1, 2, 0, 0);
    shuffleBufferManager.registerBuffer(appId2, 1, 0, 0);
    shuffleBufferManager.registerBuffer(appId2, 2, 0, 0);
    ShufflePartitionedData spd1 = createData(0, 67);
    ShufflePartitionedData spd2 = createData(0, 68);
    ShufflePartitionedData spd3 = createData(0, 68);
    ShufflePartitionedData spd4 = createData(0, 68);
    ShufflePartitionedData spd5 = createData(0, 68);
    shuffleBufferManager.cacheShuffleData(appId1, 1, false, spd1);
    shuffleBufferManager.cacheShuffleData(appId1, 2, false, spd2);
    shuffleBufferManager.cacheShuffleData(appId1, 2, false, spd3);
    shuffleBufferManager.cacheShuffleData(appId2, 1, false, spd4);

    // validate metadata of shuffle size
    Map<String, Map<Integer, AtomicLong>> shuffleSizeMap = shuffleBufferManager.getShuffleSizeMap();
    assertEquals(99, shuffleSizeMap.get(appId1).get(1).get());
    assertEquals(200, shuffleSizeMap.get(appId1).get(2).get());
    assertEquals(100, shuffleSizeMap.get(appId2).get(1).get());

    shuffleBufferManager.cacheShuffleData(appId2, 2, false, spd5);
    // flush happen
    assertEquals(99, shuffleSizeMap.get(appId1).get(1).get());
    assertEquals(0, shuffleSizeMap.get(appId1).get(2).get());
    assertEquals(0, shuffleSizeMap.get(appId2).get(1).get());
    assertEquals(0, shuffleSizeMap.get(appId2).get(1).get());
    shuffleBufferManager.releaseMemory(400, true, false);

    ShufflePartitionedData spd6 = createData(0, 300);
    shuffleBufferManager.cacheShuffleData(appId1, 1, false, spd6);
    // flush happen
    assertEquals(0, shuffleSizeMap.get(appId1).get(1).get());
    shuffleBufferManager.releaseMemory(463, true, false);

    shuffleBufferManager.cacheShuffleData(appId1, 1, false, spd1);
    shuffleBufferManager.cacheShuffleData(appId1, 2, false, spd2);
    shuffleBufferManager.cacheShuffleData(appId2, 1, false, spd4);
    shuffleBufferManager.removeBuffer(appId1);
    assertNull(shuffleSizeMap.get(appId1));
    assertEquals(100, shuffleSizeMap.get(appId2).get(1).get());
  }
}
