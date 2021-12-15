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
import com.tencent.rss.common.BufferSegment;
import com.tencent.rss.common.ShuffleDataResult;
import com.tencent.rss.common.ShufflePartitionedBlock;
import com.tencent.rss.common.ShufflePartitionedData;
import com.tencent.rss.common.util.Constants;
import com.tencent.rss.server.ShuffleDataFlushEvent;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class ShuffleBufferTest extends BufferTestBase {

  private static AtomicLong atomBlockId = new AtomicLong(0);

  @Test
  public void appendTest() {
    ShuffleBuffer shuffleBuffer = new ShuffleBuffer(100);
    shuffleBuffer.append(createData(10));
    assertEquals(42, shuffleBuffer.getSize());
    assertFalse(shuffleBuffer.isFull());

    shuffleBuffer.append(createData(26));
    assertEquals(100, shuffleBuffer.getSize());
    assertFalse(shuffleBuffer.isFull());

    shuffleBuffer.append(createData(1));
    assertEquals(133, shuffleBuffer.getSize());
    assertTrue(shuffleBuffer.isFull());
  }

  @Test
  public void toFlushEventTest() {
    ShuffleBuffer shuffleBuffer = new ShuffleBuffer(100);
    ShuffleDataFlushEvent event = shuffleBuffer.toFlushEvent("appId", 0, 0, 1, Maps.newConcurrentMap(), null);
    assertNull(event);
    shuffleBuffer.append(createData(10));
    assertEquals(42, shuffleBuffer.getSize());
    event = shuffleBuffer.toFlushEvent("appId", 0, 0, 1, Maps.newConcurrentMap(), null);
    assertEquals(42, event.getSize());
    assertEquals(0, shuffleBuffer.getSize());
    assertEquals(0, shuffleBuffer.getBlocks().size());
  }

  @Test
  public void getShuffleDataTest() {
    ShuffleBuffer shuffleBuffer = new ShuffleBuffer(200);
    // case1: cached data only, blockId = -1, readBufferSize > buffer size
    ShufflePartitionedData spd1 = createData(10);
    ShufflePartitionedData spd2 = createData(20);
    shuffleBuffer.append(spd1);
    shuffleBuffer.append(spd2);
    byte[] expectedData = getExpectedData(spd1, spd2);
    ShuffleDataResult sdr = shuffleBuffer.getShuffleData(Constants.INVALID_BLOCK_ID, 40);
    compareBufferSegment(shuffleBuffer.getBlocks(), sdr.getBufferSegments(), 0, 2);
    assertArrayEquals(expectedData, sdr.getData());

    // case2: cached data only, blockId = -1, readBufferSize = buffer size
    shuffleBuffer = new ShuffleBuffer(200);
    spd1 = createData(20);
    spd2 = createData(20);
    shuffleBuffer.append(spd1);
    shuffleBuffer.append(spd2);
    expectedData = getExpectedData(spd1, spd2);
    sdr = shuffleBuffer.getShuffleData(Constants.INVALID_BLOCK_ID, 40);
    compareBufferSegment(shuffleBuffer.getBlocks(), sdr.getBufferSegments(), 0, 2);
    assertArrayEquals(expectedData, sdr.getData());

    // case3-1: cached data only, blockId = -1, readBufferSize < buffer size
    shuffleBuffer = new ShuffleBuffer(200);
    spd1 = createData(20);
    spd2 = createData(21);
    shuffleBuffer.append(spd1);
    shuffleBuffer.append(spd2);
    expectedData = getExpectedData(spd1, spd2);
    sdr = shuffleBuffer.getShuffleData(Constants.INVALID_BLOCK_ID, 40);
    compareBufferSegment(shuffleBuffer.getBlocks(), sdr.getBufferSegments(), 0, 2);
    assertArrayEquals(expectedData, sdr.getData());

    // case3-2: cached data only, blockId = -1, readBufferSize < buffer size
    shuffleBuffer = new ShuffleBuffer(200);
    spd1 = createData(15);
    spd2 = createData(15);
    ShufflePartitionedData spd3 = createData(15);
    shuffleBuffer.append(spd1);
    shuffleBuffer.append(spd2);
    shuffleBuffer.append(spd3);
    expectedData = getExpectedData(spd1, spd2);
    sdr = shuffleBuffer.getShuffleData(Constants.INVALID_BLOCK_ID, 25);
    compareBufferSegment(shuffleBuffer.getBlocks(), sdr.getBufferSegments(), 0, 2);
    assertArrayEquals(expectedData, sdr.getData());

    // case4: cached data only, blockId != -1 && exist, readBufferSize < buffer size
    long lastBlockId = spd2.getBlockList()[0].getBlockId();
    sdr = shuffleBuffer.getShuffleData(lastBlockId, 25);
    expectedData = getExpectedData(spd3);
    compareBufferSegment(shuffleBuffer.getBlocks(), sdr.getBufferSegments(), 2, 1);
    assertArrayEquals(expectedData, sdr.getData());

    // case5: flush data only, blockId = -1, readBufferSize < buffer size
    shuffleBuffer = new ShuffleBuffer(200);
    spd1 = createData(15);
    spd2 = createData(15);
    shuffleBuffer.append(spd1);
    shuffleBuffer.append(spd2);
    ShuffleDataFlushEvent event1 = shuffleBuffer.toFlushEvent("appId", 0, 0, 1, Maps.newConcurrentMap(),null);
    assertEquals(0, shuffleBuffer.getBlocks().size());
    sdr = shuffleBuffer.getShuffleData(Constants.INVALID_BLOCK_ID, 20);
    expectedData = getExpectedData(spd1, spd2);
    compareBufferSegment(shuffleBuffer.getInFlushBlockMap().get(event1.getEventId()),
        sdr.getBufferSegments(), 0, 2);
    assertArrayEquals(expectedData, sdr.getData());

    // case5: flush data only, blockId = lastBlockId
    sdr = shuffleBuffer.getShuffleData(spd2.getBlockList()[0].getBlockId(), 20);
    assertEquals(0, sdr.getBufferSegments().size());

    // case6: no data in buffer & flush buffer
    shuffleBuffer = new ShuffleBuffer(200);
    sdr = shuffleBuffer.getShuffleData(Constants.INVALID_BLOCK_ID, 10);
    assertEquals(0, sdr.getBufferSegments().size());
    assertEquals(0, sdr.getData().length);

    // case7: get data with multiple flush buffer and cached buffer
    shuffleBuffer = new ShuffleBuffer(200);
    spd1 = createData(15);
    spd2 = createData(15);
    shuffleBuffer.append(spd1);
    shuffleBuffer.append(spd2);
    event1 = shuffleBuffer.toFlushEvent("appId", 0, 0, 1, Maps.newConcurrentMap(), null);
    spd3 = createData(15);
    ShufflePartitionedData spd4 = createData(15);
    shuffleBuffer.append(spd3);
    shuffleBuffer.append(spd4);
    ShuffleDataFlushEvent event2 = shuffleBuffer.toFlushEvent("appId", 0, 0, 1, Maps.newConcurrentMap(),null);
    ShufflePartitionedData spd5 = createData(15);
    ShufflePartitionedData spd6 = createData(15);
    shuffleBuffer.append(spd5);
    shuffleBuffer.append(spd6);
    ShuffleDataFlushEvent event3 = shuffleBuffer.toFlushEvent("appId", 0, 0, 1, Maps.newConcurrentMap(),null);
    ShufflePartitionedData spd7 = createData(15);
    ShufflePartitionedData spd8 = createData(15);
    shuffleBuffer.append(spd7);
    shuffleBuffer.append(spd8);
    ShuffleDataFlushEvent event4 = shuffleBuffer.toFlushEvent("appId", 0, 0, 1, Maps.newConcurrentMap(),null);
    ShufflePartitionedData spd9 = createData(15);
    ShufflePartitionedData spd10 = createData(15);
    shuffleBuffer.append(spd9);
    shuffleBuffer.append(spd10);
    assertEquals(2, shuffleBuffer.getBlocks().size());
    assertEquals(4, shuffleBuffer.getInFlushBlockMap().size());

    // all data in shuffle buffer as following:
    // flush event1 -> spd1, spd2
    // flush event2 -> spd3, spd4
    // flush event3 -> spd5, spd6
    // flush event4 -> spd7, spd8
    // cached buffer -> spd9, spd10
    // case7 to get spd1
    List<ShufflePartitionedBlock> expectedBlocks = Lists.newArrayList(
        shuffleBuffer.getInFlushBlockMap().get(event1.getEventId()));
    expectedData = getExpectedData(spd1);
    sdr = shuffleBuffer.getShuffleData(Constants.INVALID_BLOCK_ID, 10);
    compareBufferSegment(expectedBlocks,
        sdr.getBufferSegments(), 0, 1);
    assertArrayEquals(expectedData, sdr.getData());

    // case7 to get spd2
    expectedBlocks = Lists.newArrayList(
        shuffleBuffer.getInFlushBlockMap().get(event1.getEventId()));
    expectedData = getExpectedData(spd2);
    sdr = shuffleBuffer.getShuffleData(spd1.getBlockList()[0].getBlockId(), 10);
    compareBufferSegment(expectedBlocks,
        sdr.getBufferSegments(), 1, 1);
    assertArrayEquals(expectedData, sdr.getData());

    // case7 to get spd1, spd2
    expectedBlocks = Lists.newArrayList(
        shuffleBuffer.getInFlushBlockMap().get(event1.getEventId()));
    expectedData = getExpectedData(spd1, spd2);
    sdr = shuffleBuffer.getShuffleData(Constants.INVALID_BLOCK_ID, 20);
    compareBufferSegment(expectedBlocks,
        sdr.getBufferSegments(), 0, 2);
    assertArrayEquals(expectedData, sdr.getData());

    // case7 to get spd1, spd2, spd3
    expectedBlocks = Lists.newArrayList(
        shuffleBuffer.getInFlushBlockMap().get(event1.getEventId()));
    expectedBlocks.addAll(shuffleBuffer.getInFlushBlockMap().get(event2.getEventId()));
    expectedData = getExpectedData(spd1, spd2, spd3);
    sdr = shuffleBuffer.getShuffleData(Constants.INVALID_BLOCK_ID, 40);
    compareBufferSegment(expectedBlocks,
        sdr.getBufferSegments(), 0, 3);
    assertArrayEquals(expectedData, sdr.getData());

    // case7 to get spd2, spd3
    expectedBlocks = Lists.newArrayList(
        shuffleBuffer.getInFlushBlockMap().get(event1.getEventId()));
    expectedBlocks.addAll(shuffleBuffer.getInFlushBlockMap().get(event2.getEventId()));
    expectedData = getExpectedData(spd2, spd3);
    sdr = shuffleBuffer.getShuffleData(spd1.getBlockList()[0].getBlockId(), 20);
    compareBufferSegment(expectedBlocks,
        sdr.getBufferSegments(), 1, 2);
    assertArrayEquals(expectedData, sdr.getData());

    // case7 to get spd3
    expectedBlocks = Lists.newArrayList(
        shuffleBuffer.getInFlushBlockMap().get(event2.getEventId()));
    expectedData = getExpectedData(spd3);
    sdr = shuffleBuffer.getShuffleData(spd2.getBlockList()[0].getBlockId(), 10);
    compareBufferSegment(expectedBlocks,
        sdr.getBufferSegments(), 0, 1);
    assertArrayEquals(expectedData, sdr.getData());

    // case7 to get spd4
    expectedBlocks = Lists.newArrayList(
        shuffleBuffer.getInFlushBlockMap().get(event2.getEventId()));
    expectedData = getExpectedData(spd4);
    sdr = shuffleBuffer.getShuffleData(spd3.getBlockList()[0].getBlockId(), 10);
    compareBufferSegment(expectedBlocks,
        sdr.getBufferSegments(), 1, 1);
    assertArrayEquals(expectedData, sdr.getData());

    // case7 to get spd3, spd4
    expectedBlocks = Lists.newArrayList(
        shuffleBuffer.getInFlushBlockMap().get(event2.getEventId()));
    expectedData = getExpectedData(spd3, spd4);
    sdr = shuffleBuffer.getShuffleData(spd2.getBlockList()[0].getBlockId(), 20);
    compareBufferSegment(expectedBlocks,
        sdr.getBufferSegments(), 0, 2);
    assertArrayEquals(expectedData, sdr.getData());

    // case7 to get spd2, spd3, spd4, spd5
    expectedBlocks = Lists.newArrayList(
        shuffleBuffer.getInFlushBlockMap().get(event1.getEventId()));
    expectedBlocks.addAll(shuffleBuffer.getInFlushBlockMap().get(event2.getEventId()));
    expectedBlocks.addAll(shuffleBuffer.getInFlushBlockMap().get(event3.getEventId()));
    expectedData = getExpectedData(spd2, spd3, spd4, spd5);
    sdr = shuffleBuffer.getShuffleData(spd1.getBlockList()[0].getBlockId(), 60);
    compareBufferSegment(expectedBlocks,
        sdr.getBufferSegments(), 1, 4);
    assertArrayEquals(expectedData, sdr.getData());

    // case7 to get spd4, spd5
    expectedBlocks = Lists.newArrayList(
        shuffleBuffer.getInFlushBlockMap().get(event2.getEventId()));
    expectedBlocks.addAll(shuffleBuffer.getInFlushBlockMap().get(event3.getEventId()));
    expectedData = getExpectedData(spd4, spd5);
    sdr = shuffleBuffer.getShuffleData(spd3.getBlockList()[0].getBlockId(), 20);
    compareBufferSegment(expectedBlocks,
        sdr.getBufferSegments(), 1, 2);
    assertArrayEquals(expectedData, sdr.getData());

    // case7 to get spd4, spd5, spd6
    expectedBlocks = Lists.newArrayList(
        shuffleBuffer.getInFlushBlockMap().get(event2.getEventId()));
    expectedBlocks.addAll(shuffleBuffer.getInFlushBlockMap().get(event3.getEventId()));
    expectedData = getExpectedData(spd4, spd5, spd6);
    sdr = shuffleBuffer.getShuffleData(spd3.getBlockList()[0].getBlockId(), 40);
    compareBufferSegment(expectedBlocks,
        sdr.getBufferSegments(), 1, 3);
    assertArrayEquals(expectedData, sdr.getData());

    // case7 to get spd6, spd7
    expectedBlocks = Lists.newArrayList(
        shuffleBuffer.getInFlushBlockMap().get(event3.getEventId()));
    expectedBlocks.addAll(shuffleBuffer.getInFlushBlockMap().get(event4.getEventId()));
    expectedData = getExpectedData(spd6, spd7);
    sdr = shuffleBuffer.getShuffleData(spd5.getBlockList()[0].getBlockId(), 20);
    compareBufferSegment(expectedBlocks,
        sdr.getBufferSegments(), 1, 2);
    assertArrayEquals(expectedData, sdr.getData());

    // case7 to get spd7
    expectedBlocks = Lists.newArrayList(
        shuffleBuffer.getInFlushBlockMap().get(event4.getEventId()));
    expectedData = getExpectedData(spd7);
    sdr = shuffleBuffer.getShuffleData(spd6.getBlockList()[0].getBlockId(), 10);
    compareBufferSegment(expectedBlocks,
        sdr.getBufferSegments(), 0, 1);
    assertArrayEquals(expectedData, sdr.getData());

    // case7 to get spd8
    expectedBlocks = Lists.newArrayList(
        shuffleBuffer.getInFlushBlockMap().get(event4.getEventId()));
    expectedData = getExpectedData(spd8);
    sdr = shuffleBuffer.getShuffleData(spd7.getBlockList()[0].getBlockId(), 10);
    compareBufferSegment(expectedBlocks,
        sdr.getBufferSegments(), 1, 1);
    assertArrayEquals(expectedData, sdr.getData());

    // case7 to get spd7, spd8
    expectedBlocks = Lists.newArrayList(
        shuffleBuffer.getInFlushBlockMap().get(event4.getEventId()));
    expectedData = getExpectedData(spd7, spd8);
    sdr = shuffleBuffer.getShuffleData(spd6.getBlockList()[0].getBlockId(), 20);
    compareBufferSegment(expectedBlocks,
        sdr.getBufferSegments(), 0, 2);
    assertArrayEquals(expectedData, sdr.getData());

    // case7 to get spd8, spd9
    expectedBlocks = Lists.newArrayList(
        shuffleBuffer.getInFlushBlockMap().get(event4.getEventId()));
    expectedBlocks.addAll(shuffleBuffer.getBlocks());
    expectedData = getExpectedData(spd8, spd9);
    sdr = shuffleBuffer.getShuffleData(spd7.getBlockList()[0].getBlockId(), 20);
    compareBufferSegment(expectedBlocks,
        sdr.getBufferSegments(), 1, 2);
    assertArrayEquals(expectedData, sdr.getData());

    // case7 to get spd9
    expectedBlocks = Lists.newArrayList(shuffleBuffer.getBlocks());
    expectedBlocks.addAll(shuffleBuffer.getBlocks());
    expectedData = getExpectedData(spd9);
    sdr = shuffleBuffer.getShuffleData(spd8.getBlockList()[0].getBlockId(), 10);
    compareBufferSegment(expectedBlocks,
        sdr.getBufferSegments(), 0, 1);
    assertArrayEquals(expectedData, sdr.getData());

    // case7 to get spd8, spd9, spd10
    expectedBlocks = Lists.newArrayList(
        shuffleBuffer.getInFlushBlockMap().get(event4.getEventId()));
    expectedBlocks.addAll(shuffleBuffer.getBlocks());
    expectedData = getExpectedData(spd8, spd9, spd10);
    sdr = shuffleBuffer.getShuffleData(spd7.getBlockList()[0].getBlockId(), 40);
    compareBufferSegment(expectedBlocks,
        sdr.getBufferSegments(), 1, 3);
    assertArrayEquals(expectedData, sdr.getData());

    // case7 to get spd1 - spd10
    expectedBlocks = Lists.newArrayList(
        shuffleBuffer.getInFlushBlockMap().get(event1.getEventId()));
    expectedBlocks.addAll(shuffleBuffer.getInFlushBlockMap().get(event2.getEventId()));
    expectedBlocks.addAll(shuffleBuffer.getInFlushBlockMap().get(event3.getEventId()));
    expectedBlocks.addAll(shuffleBuffer.getInFlushBlockMap().get(event4.getEventId()));
    expectedBlocks.addAll(shuffleBuffer.getBlocks());
    expectedData = getExpectedData(spd1, spd2, spd3, spd4, spd5, spd6, spd7, spd8, spd9, spd10);
    sdr = shuffleBuffer.getShuffleData(Constants.INVALID_BLOCK_ID, 150);
    compareBufferSegment(expectedBlocks,
        sdr.getBufferSegments(), 0, 10);
    assertArrayEquals(expectedData, sdr.getData());

    // case7 after get spd10
    sdr = shuffleBuffer.getShuffleData(spd10.getBlockList()[0].getBlockId(), 20);
    assertEquals(0, sdr.getBufferSegments().size());

    // case7 can't find blockId, read from start
    expectedBlocks = Lists.newArrayList(
        shuffleBuffer.getInFlushBlockMap().get(event1.getEventId()));
    expectedData = getExpectedData(spd1, spd2);
    sdr = shuffleBuffer.getShuffleData(-200, 20);
    compareBufferSegment(expectedBlocks,
        sdr.getBufferSegments(), 0, 2);
    assertArrayEquals(expectedData, sdr.getData());
  }

  private byte[] getExpectedData(ShufflePartitionedData... spds ) {
    int size = 0;
    for (ShufflePartitionedData spd : spds) {
      size += spd.getBlockList()[0].getLength();
    }
    byte[] expectedData = new byte[size];
    int offset = 0;
    for (ShufflePartitionedData spd : spds) {
      ShufflePartitionedBlock block = spd.getBlockList()[0];
      System.arraycopy(block.getData(), 0, expectedData, offset, block.getLength());
      offset += block.getLength();
    }
    return expectedData;
  }

  private void compareBufferSegment(List<ShufflePartitionedBlock> blocks,
      List<BufferSegment> bufferSegments, int startBlockIndex, int expectedBlockNum) {
    int segmentIndex = 0;
    int offset = 0;
    assertEquals(expectedBlockNum, bufferSegments.size());
    for (int i = startBlockIndex; i < startBlockIndex + expectedBlockNum; i++) {
      ShufflePartitionedBlock spb = blocks.get(i);
      BufferSegment segment = bufferSegments.get(segmentIndex);
      assertEquals(spb.getBlockId(), segment.getBlockId());
      assertEquals(spb.getLength(), segment.getLength());
      assertEquals(spb.getCrc(), segment.getCrc());
      assertEquals(offset, segment.getOffset());
      offset += spb.getLength();
      segmentIndex++;
    }
  }
}
