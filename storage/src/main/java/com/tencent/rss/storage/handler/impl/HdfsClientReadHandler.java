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
import com.tencent.rss.common.util.Constants;
import com.tencent.rss.storage.util.ShuffleStorageUtils;

import java.io.IOException;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HdfsClientReadHandler extends AbstractHdfsClientReadHandler {

  private static final Logger LOG = LoggerFactory.getLogger(HdfsClientReadHandler.class);

  private final List<HdfsShuffleFileReadHandler> hdfsShuffleFileReadHandlers = Lists.newArrayList();
  private int readHandlerIndex;

  public HdfsClientReadHandler(
      String appId,
      int shuffleId,
      int partitionId,
      int indexReadLimit,
      int partitionNumPerRange,
      int partitionNum,
      int readBufferSize,
      String storageBasePath,
      Configuration hadoopConf) {
    this.appId = appId;
    this.shuffleId = shuffleId;
    this.partitionId = partitionId;
    this.indexReadLimit = indexReadLimit;
    this.partitionNumPerRange = partitionNumPerRange;
    this.partitionNum = partitionNum;
    this.readBufferSize = readBufferSize;
    this.storageBasePath = storageBasePath;
    this.hadoopConf = hadoopConf;
    this.readHandlerIndex = 0;
    String fullShufflePath = ShuffleStorageUtils.getFullShuffleDataFolder(storageBasePath,
        ShuffleStorageUtils.getShuffleDataPathWithRange(appId,
            shuffleId, partitionId, partitionNumPerRange, partitionNum));
    init(fullShufflePath);
  }

  protected void init(String fullShufflePath) {
    FileSystem fs;
    Path baseFolder = new Path(fullShufflePath);
    try {
      fs = ShuffleStorageUtils.getFileSystemForPath(baseFolder, hadoopConf);
    } catch (IOException ioe) {
      throw new RuntimeException("Can't get FileSystem for " + baseFolder);
    }

    FileStatus[] indexFiles;
    String failedGetIndexFileMsg = "Can't list index file in  " + baseFolder;

    try {
      // get all index files
      indexFiles = fs.listStatus(baseFolder,
          file -> file.getName().endsWith(Constants.SHUFFLE_INDEX_FILE_SUFFIX));
    } catch (Exception e) {
      LOG.error(failedGetIndexFileMsg, e);
      return;
    }

    if (indexFiles != null) {
      for (FileStatus status : indexFiles) {
        LOG.info("Find index file for shuffleId[" + shuffleId + "], partitionId["
            + partitionId + "] " + status.getPath());
        String fileNamePrefix = getFileNamePrefix(status.getPath().getName());
        try {
          HdfsFileReader indexReader = createHdfsReader(
              fullShufflePath, ShuffleStorageUtils.generateIndexFileName(fileNamePrefix), hadoopConf);
          HdfsFileReader dataReader = createHdfsReader(
              fullShufflePath, ShuffleStorageUtils.generateDataFileName(fileNamePrefix), hadoopConf);
          HdfsShuffleFileReadHandler hdfsShuffleFileReader =
              new HdfsShuffleFileReadHandler(fileNamePrefix, indexReader, dataReader, readBufferSize);
          hdfsShuffleFileReadHandlers.add(hdfsShuffleFileReader);
        } catch (Exception e) {
          LOG.warn("Can't create ShuffleReaderHandler for " + fileNamePrefix, e);
        }
      }
    }
  }

  // TODO: remove the useless segmentIndex
  @Override
  public ShuffleDataResult readShuffleData(int segmentIndex) {
    if (readHandlerIndex >= hdfsShuffleFileReadHandlers.size()) {
      return null;
    }

    HdfsShuffleFileReadHandler hdfsShuffleFileReader = hdfsShuffleFileReadHandlers.get(readHandlerIndex);
    ShuffleDataResult shuffleDataResult = hdfsShuffleFileReader.readShuffleData();

    while (shuffleDataResult == null) {
      ++readHandlerIndex;
      if (readHandlerIndex >= hdfsShuffleFileReadHandlers.size()) {
        return null;
      }
      hdfsShuffleFileReader = hdfsShuffleFileReadHandlers.get(readHandlerIndex);
      shuffleDataResult = hdfsShuffleFileReader.readShuffleData();
    }

    return shuffleDataResult;
  }

  @Override
  public synchronized void close() {
    for (HdfsShuffleFileReadHandler handler : hdfsShuffleFileReadHandlers) {
      handler.close();
    }
  }

  protected List<HdfsShuffleFileReadHandler> getHdfsShuffleFileReadHandlers() {
    return hdfsShuffleFileReadHandlers;
  }

  protected int getReadHandlerIndex() {
    return readHandlerIndex;
  }
}
