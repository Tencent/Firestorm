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

import com.google.common.collect.Lists;
import com.tencent.rss.common.ShuffleDataResult;
import com.tencent.rss.common.ShuffleDataSegment;
import com.tencent.rss.common.ShuffleIndexResult;
import com.tencent.rss.common.util.RssUtils;
import java.io.IOException;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * HdfsShuffleFileReadHandler is a shuffle-specific file read handler, it contains two HdfsFileReader
 * created by using the index file and its indexed data file.
 */
public class HdfsShuffleFileReadHandler {
  private static final Logger LOG = LoggerFactory.getLogger(HdfsShuffleFileReadHandler.class);

  private final String filePrefix;
  private final HdfsFileReader indexReader;
  private final HdfsFileReader dataReader;
  private final int readBufferSize;

  private final List<ShuffleDataSegment> shuffleDataSegments = Lists.newArrayList();
  private int segmentIndex;

  public HdfsShuffleFileReadHandler(
      String filePrefix, HdfsFileReader indexReader, HdfsFileReader dataReader, int readBufferSize) {
    this.filePrefix = filePrefix;
    this.readBufferSize = readBufferSize;
    this.indexReader = indexReader;
    this.dataReader = dataReader;
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

    byte[] data = dataReader.readData(shuffleDataSegment.getOffset(), expectedLength);
    if (data.length != expectedLength) {
      LOG.warn("Fail to read expected[{}] data, actual[{}] and segment is {} from file {}.data",
          expectedLength, data.length, shuffleDataSegment, filePrefix);
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

  private ShuffleIndexResult readShuffleIndex() {
    long start = System.currentTimeMillis();
    try {
      byte[] indexData = indexReader.readIndex();
      LOG.info("Read index files {}.index for {} ms", filePrefix, System.currentTimeMillis() - start);
      return new ShuffleIndexResult(indexData);
    } catch (Exception e) {
      LOG.info("Fail to read index files {}.index", filePrefix);
    }
    return new ShuffleIndexResult();
  }

  public synchronized void close() {
    try {
      dataReader.close();
    } catch (IOException ioe) {
      String message = "Error happened when close index filer reader for " + filePrefix + ".data";
      LOG.warn(message, ioe);
    }

    try {
      indexReader.close();
    } catch (IOException ioe) {
      String message = "Error happened when close data file reader for " + filePrefix + ".index";
      LOG.warn(message, ioe);
    }
  }

  public List<ShuffleDataSegment> getShuffleDataSegments() {
    return shuffleDataSegments;
  }
}
