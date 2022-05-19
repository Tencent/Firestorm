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

import com.tencent.rss.common.RssShuffleUtils;
import com.tencent.rss.common.exception.RssException;

// In MR shuffle, MapOutput encapsulates the logic to fetch map task's output data via http.
// So, in RSS, we should bypass this logic, and directly write data to MapOutput.
public class RssBypassWriter {
  private static final Log LOG = LogFactory.getLog(RssBypassWriter.class);

  public static void write(MapOutput mapOutput, byte[] compressedData,
                           int unCompressedLength) {
    // Write and commit uncompressed data to MapOutput.
    // In the majority of cases, merger allocates memory to accept data,
    // but when data size exceeds the threshold, merger can also allocate disk.
    // So, we should consider the two situations, respectively.
    if (mapOutput instanceof InMemoryMapOutput) {
      write((InMemoryMapOutput) mapOutput, compressedData, unCompressedLength);
    } else if (mapOutput instanceof OnDiskMapOutput) {
      write((OnDiskMapOutput) mapOutput, compressedData, unCompressedLength);
    } else {
      throw new IllegalStateException("Merger reserve unknown type of MapOutput："
        + mapOutput.getClass().getCanonicalName());
    }
  }

  private static void write(InMemoryMapOutput inMemoryMapOutput,
                            byte[] compressedData,
                            int unCompressedLength) {
    byte[] mrMemoryBuffer = inMemoryMapOutput.getMemory();
    // we decompress data on MR's memory directly to avoid extra allocate and copy
    RssShuffleUtils.inplaceDecompressData(
      ByteBuffer.wrap(mrMemoryBuffer),
      ByteBuffer.wrap(compressedData),
      unCompressedLength);
  }

  private static void write(OnDiskMapOutput onDiskMapOutput,
                            byte[] compressedData,
                            int unCompressedLength) {
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

    // Copy data to local-disk, we still need to allocate buffer
    byte[] uncompressed = RssShuffleUtils.decompressData(
      ByteBuffer.wrap(compressedData), unCompressedLength, false).array();
    try {
      disk.write(uncompressed, 0, unCompressedLength);
      disk.close();
    } catch (IOException ioe) {
      // Close the streams
      IOUtils.cleanup(LOG, disk);
      throw new RssException("Failed to write OnDiskMapOutput due to: "
        + ioe.getMessage());
    }
  }
}
