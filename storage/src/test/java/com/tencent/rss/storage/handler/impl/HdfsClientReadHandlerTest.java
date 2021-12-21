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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.tencent.rss.common.BufferSegment;
import com.tencent.rss.common.ShuffleDataResult;
import com.tencent.rss.storage.HdfsShuffleHandlerTestBase;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import org.junit.Test;

public class HdfsClientReadHandlerTest extends HdfsShuffleHandlerTestBase {

  @Test
  public void test() {
    try {
      String basePath = HDFS_URI + "clientReadTest1";
      HdfsShuffleWriteHandler writeHandler =
          new HdfsShuffleWriteHandler(
              "appId",
              0,
              1,
              1,
              basePath,
              "test",
              conf);

      Map<Long, byte[]> expectedData = Maps.newHashMap();

      int readBufferSize = 13;
      int total = 0;
      int totalBlockNum = 0;
      int expectTotalBlockNum = 0;
      for (int i = 0; i < 5; i++) {
        writeHandler.setFailTimes(i);
        int num = new Random().nextInt(17);
        writeTestData(writeHandler,  num, 3, 0, expectedData);
        total += calcExpectedSegmentNum(num, 3, readBufferSize);
        expectTotalBlockNum += num;
      }

      HdfsClientReadHandler handler = new HdfsClientReadHandler(
          "appId",
          0,
          1,
          1024 * 10214,
          1,
          10,
          readBufferSize,
          basePath,
          conf);
      Set<Long> actualBlockIds = Sets.newHashSet();

      for (int i = 0; i < total; ++i) {
        ShuffleDataResult shuffleDataResult = handler.readShuffleData();
        totalBlockNum += shuffleDataResult.getBufferSegments().size();
        checkData(shuffleDataResult, expectedData);
        for (BufferSegment bufferSegment : shuffleDataResult.getBufferSegments()) {
          actualBlockIds.add(bufferSegment.getBlockId());
        }
      }

      assertNull(handler.readShuffleData());
      assertEquals(
          total,
          handler.getHdfsShuffleFileReadHandlers()
              .stream()
              .mapToInt(i -> i.getShuffleDataSegments().size())
              .sum());
      assertEquals(expectTotalBlockNum, totalBlockNum);
      assertEquals(expectedData.keySet(), actualBlockIds);
      assertEquals(5, handler.getReadHandlerIndex());
      handler.close();
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }
}
