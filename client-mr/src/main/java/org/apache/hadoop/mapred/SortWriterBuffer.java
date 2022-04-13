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

package org.apache.hadoop.mapred;

import com.google.common.collect.Lists;
import org.apache.hadoop.io.RawComparator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Comparator;
import java.util.List;

public class SortWriterBuffer<K, V> extends OutputStream  {

  private static final Logger LOG = LoggerFactory.getLogger(SortWriterBuffer.class);
  private long copyTime = 0;
  private final List<WrappedBuffer> buffers = Lists.newArrayList();
  private final List<Record<K>> records = Lists.newArrayList();
  private int dataLength = 0;
  private long sortTime = 0;
  private final RawComparator<K> comparator;
  private long maxBufferSize;
  private int partitionId;

  public SortWriterBuffer(int partitionId, RawComparator<K> comparator) {
    this.partitionId = partitionId;
    this.comparator = comparator;
  }

  public synchronized void addRecord(K key, long start, long end) {
    records.add(new Record<K>(key, start, end));
  }

  public synchronized byte[] getData() {
    byte[] data = new byte[dataLength];
    int offset = 0;
    long startSort = System.currentTimeMillis();
    records.sort(new Comparator<Record<K>>() {
      @Override
      public int compare(Record<K> o1, Record<K> o2) {
        return comparator.compare(o1.getKey(), o2.getKey());
      }
    });

    long startCopy =  System.currentTimeMillis();
    sortTime += startCopy - startSort;
    for (Record<K> record : records) {
      int beginIndex = (int) (record.getStart() / maxBufferSize);
      int beginOffSet = (int) (record.getStart() % maxBufferSize);
      int endIndex = (int) (record.getEnd() / maxBufferSize);
      int endOffset = (int) (record.getEnd() % maxBufferSize);
      if (beginIndex == endIndex) {
        int length = endOffset - beginOffSet;
        System.arraycopy(buffers.get(beginIndex).getBuffer(), beginOffSet, data, offset, length);
        offset += length;
      } else {
        int finalBeginOffset = beginOffSet;
        for (int j = beginIndex; j <= endIndex; j++){
          int finalEndOffset = (int) ((j == endIndex) ? endOffset : maxBufferSize);
          int length = finalEndOffset - finalBeginOffset;
          System.arraycopy(buffers.get(j).getBuffer(), finalBeginOffset, data, offset, length);
          offset += length;
          finalBeginOffset = 0;
        }
      }
    }

    copyTime += System.currentTimeMillis() - startCopy;
    return data;
  }

  public int getDataLength() {
    return dataLength;
  }

  public long getCopyTime() {
    return copyTime;
  }

  public long getSortTime() {
    return sortTime;
  }

  public int getPartitionId() {
    return partitionId;
  }

  @Override
  public void write(int b) throws IOException {
    if (dataLength + 4 > buffers.size() * maxBufferSize) {
      buffers.add(new WrappedBuffer((int)maxBufferSize));
    }
    int index = (int) (dataLength / maxBufferSize);
    int offset = (int) (dataLength / maxBufferSize);
    WrappedBuffer buffer = buffers.get(index);
    buffer.getBuffer()[offset] = (byte) b;
    dataLength++;
  }

  @Override
  public void write(byte b[], int off, int len) throws IOException {
    if (b == null) {
      throw new NullPointerException();
    } else if ((off < 0) || (off > b.length) || (len < 0) ||
        ((off + len) > b.length) || ((off + len) < 0)) {
      throw new IndexOutOfBoundsException();
    } else if (len == 0) {
      return;
    }
    int bufferNum = (int)((dataLength + len) / maxBufferSize) + 1 - buffers.size();
    for (int i = 0; i < bufferNum; i++) {
      buffers.add(new WrappedBuffer((int)maxBufferSize));
    }
    int index = (int) (dataLength / maxBufferSize);
    int offset = (int) (dataLength / maxBufferSize);
    int srcPos = 0;
    while(len > 0) {
      int copyLength = len < maxBufferSize ? (len - offset) : (int) (maxBufferSize - offset);
      System.arraycopy(b, srcPos, buffers.get(index), offset, copyLength);
      offset = 0;
      srcPos += copyLength;
      index++;
      len -= copyLength;
      dataLength += copyLength;
    }
  }

  private static final class Record<K> {

    private K key;
    private long start;
    private long end;

    public Record(K key, long start, long end) {
      this.key = key;
      this.start = start;
      this.end = end;
    }

    public K getKey() {
      return key;
    }

    public long getStart() {
      return start;
    }

    public long getEnd() {
      return end;
    }
  }

  private static final class WrappedBuffer {

    private byte[] buffer;
    private int size;

    WrappedBuffer(int size) {
      this.buffer = new byte[size];
      this.size = size;
    }

    public byte[] getBuffer() {
      return buffer;
    }

    public int getSize() {
      return size;
    }
  }

}
