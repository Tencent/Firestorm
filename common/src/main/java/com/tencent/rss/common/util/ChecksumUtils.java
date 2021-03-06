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

package com.tencent.rss.common.util;

import java.nio.ByteBuffer;
import java.util.zip.CRC32;

public class ChecksumUtils {

  private static final int LENGTH_PER_CRC = 4 * 1024;

  public static long getCrc32(byte[] buf) {
    return getCrc32(buf, 0, buf.length);
  }

  public static long getCrc32(byte[] buf, int offset, int length) {
    CRC32 crc32 = new CRC32();

    for (int i = 0; i < length; ) {
      int len = Math.min(LENGTH_PER_CRC, length - i);
      crc32.update(buf, i + offset, len);
      i += len;
    }

    return crc32.getValue();
  }

  // you may need to flip at first
  public static long getCrc32(ByteBuffer byteBuffer) {
    if (byteBuffer.hasArray()) {
      return getCrc32(byteBuffer.array(), byteBuffer.arrayOffset() + byteBuffer.position(), byteBuffer.remaining());
    } else {
      byte[] byteArray = new byte[byteBuffer.remaining()];
      byteBuffer.get(byteArray);
      return getCrc32(byteArray);
    }
  }
}
