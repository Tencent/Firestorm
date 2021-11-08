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

import java.util.List;

public class ShuffleDataSegment {
  private String path;
  private long offset;
  private int length;
  private List<BufferSegment> bufferSegments;

  public ShuffleDataSegment(String path, long offset, int length, List<BufferSegment> bufferSegments) {
    this(offset, length, bufferSegments);
    this.path = path;
  }

  public ShuffleDataSegment(long offset, int length, List<BufferSegment> bufferSegments) {
    this.offset = offset;
    this.length = length;
    this.bufferSegments = bufferSegments;
  }

  public String getPath() {
    return path;
  }

  public long getOffset() {
    return offset;
  }

  public int getLength() {
    return length;
  }

  public List<BufferSegment> getBufferSegments() {
    return bufferSegments;
  }
}
