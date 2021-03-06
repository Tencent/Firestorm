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

import java.io.Closeable;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tencent.rss.storage.api.FileReader;

public class LocalFileReader implements FileReader, Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(HdfsFileReader.class);
  private String path;
  private DataInputStream dataInputStream;

  public LocalFileReader(String path) throws Exception {
    this.path = path;
    dataInputStream = new DataInputStream(new FileInputStream(path));
  }

  public byte[] read(long offset, int length) {
    try {
      dataInputStream.skip(offset);
      byte[] buf = new byte[length];
      dataInputStream.readFully(buf);
      return buf;
    } catch (Exception e) {
      LOG.warn("Can't read data for path:" + path + " with offset[" + offset + "], length[" + length + "]", e);
    }
    return new byte[0];
  }

  public byte[] read() {
    try {
      return IOUtils.toByteArray(dataInputStream);
    } catch (IOException e) {
      LOG.error("Fail to read all data from {}", path, e);
      return new byte[0];
    }
  }

  @Override
  public synchronized void close() {
    if (dataInputStream != null) {
      try {
        dataInputStream.close();
      } catch (IOException ioe) {
        LOG.warn("Error happen when close " + path, ioe);
      }
    }
  }
}
