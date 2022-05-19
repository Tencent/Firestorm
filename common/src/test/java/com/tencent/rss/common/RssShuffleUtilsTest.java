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

package com.tencent.rss.common;

import com.google.common.collect.Lists;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.assertArrayEquals;

public class RssShuffleUtilsTest {
  @Test
  public void compressionTest() {
    List<Integer> testSizes = Lists.newArrayList(
      1, 1024, 128 * 1024, 512 * 1024, 1024 * 1024, 4 * 1024 * 1024);
    for (int size : testSizes) {
      singleTest1(size);
      singleTest2(size);
    }
  }

  private void singleTest1(int size) {
    byte[] buf = new byte[size];
    new Random().nextBytes(buf);

    byte[] compressed = RssShuffleUtils.compressData(buf);
    byte[] uncompressed = RssShuffleUtils.decompressData(compressed, size);
    assertArrayEquals(buf, uncompressed);
  }

  private void singleTest2(int size) {
    byte[] srcBuffer = new byte[size];
    new Random().nextBytes(srcBuffer);

    byte[] desBuffer = new byte[size];

    byte[] compressed = RssShuffleUtils.compressData(srcBuffer);
    RssShuffleUtils.inplaceDecompressData(ByteBuffer.wrap(desBuffer), ByteBuffer.wrap(compressed), size);

    assertArrayEquals(srcBuffer, desBuffer);
  }
}
