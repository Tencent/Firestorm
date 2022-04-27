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

package com.tencent.rss.client.response;

import java.nio.ByteBuffer;

public class CompressedShuffleBlock {

  private ByteBuffer byteBuffer;
  private int uncompressLength;
  private long blockId;

  public CompressedShuffleBlock(ByteBuffer byteBuffer, int uncompressLength, long blockId) {
    this.byteBuffer = byteBuffer;
    this.uncompressLength = uncompressLength;
    this.blockId = blockId;
  }

  public int getUncompressLength() {
    return uncompressLength;
  }

  public ByteBuffer getByteBuffer() {
    return byteBuffer;
  }

  public long getBlockId() {
    return blockId;
  }
}
