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

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

public abstract class AbstractHdfsClientReadHandler extends AbstractFileClientReadHandler {

  protected int partitionNumPerRange;
  protected int partitionNum;
  protected int readBufferSize;
  protected String storageBasePath;
  protected Configuration hadoopConf;

  protected void init(String fullShufflePath) {

  }

  protected String getFileNamePrefix(String fileName) {
    int point = fileName.lastIndexOf(".");
    return fileName.substring(0, point);
  }

  protected HdfsFileReader createHdfsReader(
      String folder, String fileName, Configuration hadoopConf) throws IOException, IllegalStateException {
    Path path = new Path(folder, fileName);
    HdfsFileReader reader = new HdfsFileReader(path, hadoopConf);
    return reader;
  }
}
