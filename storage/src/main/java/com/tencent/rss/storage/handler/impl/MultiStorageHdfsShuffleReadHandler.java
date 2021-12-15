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

import com.tencent.rss.common.ShuffleDataResult;
import com.tencent.rss.common.ShuffleDataSegment;
import com.tencent.rss.common.ShuffleIndexResult;
import com.tencent.rss.common.util.RssUtils;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MultiStorageHdfsShuffleReadHandler extends HdfsShuffleReadHandler {

  private static final Logger LOG = LoggerFactory.getLogger(MultiStorageHdfsShuffleReadHandler.class);

  private final int partitionId;
  private long dataFileOffset;

  public MultiStorageHdfsShuffleReadHandler(
      int partitionId,
      String filePrefix,
      int readBufferSize,
      Configuration conf)  throws IOException {
    super(filePrefix, readBufferSize, conf);
    this.partitionId = partitionId;
  }

  public ShuffleDataResult readShuffleData() {
    if (shuffleDataSegments.isEmpty()) {
      ShuffleIndexResult shuffleIndexResult = readShuffleIndex();
      if (shuffleIndexResult == null || shuffleIndexResult.isEmpty()) {
        return null;
      }

      List<ShuffleDataSegment> cur = RssUtils.transIndexDataToSegments(shuffleIndexResult, readBufferSize);
      shuffleDataSegments.addAll(cur);
    }

    if (segmentIndex >= shuffleDataSegments.size()) {
      return null;
    }
    ShuffleDataSegment shuffleDataSegment = shuffleDataSegments.get(segmentIndex++);

    // Here we make an assumption that the rest of the file is corrupted, if an unexpected data is read.
    int expectedLength = shuffleDataSegment.getLength();
    if (expectedLength <= 0) {
      LOG.warn("Invalid data segment is {} from file {}.data", shuffleDataSegment, filePrefix);
      return null;
    }

    byte[] data = dataReader.read(dataFileOffset + shuffleDataSegment.getOffset(), expectedLength);
    if (data.length != expectedLength) {
      LOG.warn("Fail to read expected[{}] data, actual[{}], offset[{}] and segment is {} from file {}.data",
          expectedLength, data.length, dataFileOffset, shuffleDataSegment, filePrefix);
      return null;
    }

    ShuffleDataResult shuffleDataResult = new ShuffleDataResult(data, shuffleDataSegment.getBufferSegments());
    if (shuffleDataResult.isEmpty()) {
      LOG.warn("Shuffle data is empty, expected length {}, data length {}, segment {} in file {}.data",
          expectedLength, data.length, shuffleDataSegment, filePrefix);
      return null;
    }

    return shuffleDataResult;
  }

  protected ShuffleIndexResult readShuffleIndex() {
    long start = System.currentTimeMillis();
    try {
      byte[] indexData = indexReader.read();

      ByteBuffer byteBuffer = ByteBuffer.wrap(indexData);
      ShuffleIndexHeader shuffleIndexHeader = ShuffleIndexHeader.extractHeader(byteBuffer);
      if (shuffleIndexHeader == null) {
        LOG.error("Fail to read index from {}.index", filePrefix);
        return new ShuffleIndexResult();
      }

      int indexFileOffset = shuffleIndexHeader.getHeaderLen();
      int indexPartitionLen = 0;
      long dataFileOffset = 0;
      for (ShuffleIndexHeader.Entry entry : shuffleIndexHeader.getIndexes()) {
        int partitionId = entry.getPartitionId();
        indexPartitionLen = (int) entry.getPartitionIndexLength();
        if (partitionId != this.partitionId) {
          indexFileOffset += entry.getPartitionIndexLength();
          dataFileOffset += entry.getPartitionDataLength();
          continue;
        }

        if ((indexFileOffset + indexPartitionLen) >= indexData.length) {
          LOG.error("Index of partition {} is invalid, offset = {}, length = {} in {}.index",
              partitionId, indexFileOffset, indexPartitionLen, filePrefix);
        }

        LOG.info("Read index files {}.index for {} ms", filePrefix, System.currentTimeMillis() - start);
        this.dataFileOffset = dataFileOffset;
        return new ShuffleIndexResult(
            Arrays.copyOfRange(indexData, indexFileOffset, indexFileOffset + indexPartitionLen));
      }
    } catch (Exception e) {
      LOG.info("Fail to read index files {}.index", filePrefix);
    }
    return new ShuffleIndexResult();
  }


}
