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

package org.apache.hadoop.mapreduce.task.reduce;

import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IOUtils;

import com.tencent.rss.common.exception.RssException;

public class RssBypassWriter {
  private static final Log LOG = LogFactory.getLog(RssBypassWriter.class);

  public static void write(InMemoryMapOutput inMemoryMapOutput, ByteBuffer buffer) {
    byte[] memory = inMemoryMapOutput.getMemory();
    System.arraycopy(buffer.array(),0, memory, 0, buffer.capacity());
  }

  public static void write(OnDiskMapOutput onDiskMapOutput, ByteBuffer buffer) {
    OutputStream disk = null;
    try {
      Class clazz = Class.forName(OnDiskMapOutput.class.getName());
      Field diskField = clazz.getDeclaredField("disk");
      diskField.setAccessible(true);
      disk = (OutputStream)diskField.get(onDiskMapOutput);
    } catch (Exception e) {
      throw new RssException("Failed to access OnDiskMapOutput by reflection due to: "
        + e.getMessage());
    }
    if (disk == null) {
      throw new RssException("OnDiskMapOutput should not contain null disk stream");
    }

    // Copy data to local-disk
    try {
      disk.write(buffer.array(), 0, buffer.capacity());
      disk.close();
    } catch (IOException ioe) {
      // Close the streams
      IOUtils.cleanup(LOG, disk);
      throw new RssException("Failed to write OnDiskMapOutput due to: "
        + ioe.getMessage());
    }
  }
}
