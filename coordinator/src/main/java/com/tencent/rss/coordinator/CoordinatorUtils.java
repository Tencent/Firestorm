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

package com.tencent.rss.coordinator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Maps;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tencent.rss.common.PartitionRange;
import com.tencent.rss.common.util.Constants;
import com.tencent.rss.proto.RssProtos;
import com.tencent.rss.proto.RssProtos.GetShuffleAssignmentsResponse;

public class CoordinatorUtils {

  private static final Logger LOG = LoggerFactory.getLogger(CoordinatorUtils.class);

  public static GetShuffleAssignmentsResponse toGetShuffleAssignmentsResponse(
      PartitionRangeAssignment pra) {
    List<RssProtos.PartitionRangeAssignment> praList = pra.convertToGrpcProto();

    return GetShuffleAssignmentsResponse.newBuilder()
        .addAllAssignments(praList)
        .build();
  }

  public static int nextIdx(int idx, int size) {
    ++idx;
    if (idx >= size) {
      idx = 0;
    }
    return idx;
  }

  public static List<PartitionRange> generateRanges(int totalPartitionNum, int partitionNumPerRange) {
    List<PartitionRange> ranges = new ArrayList<>();
    if (totalPartitionNum <= 0 || partitionNumPerRange <= 0) {
      return ranges;
    }

    for (int start = 0; start < totalPartitionNum; start += partitionNumPerRange) {
      int end = start + partitionNumPerRange - 1;
      PartitionRange range = new PartitionRange(start, end);
      ranges.add(range);
    }

    return ranges;
  }

  // TODO: the pure hdfs related classes and methods should be placed in a common module
  public static FileSystem getFileSystemForPath(Path path, Configuration conf) throws IOException {
    // For local file systems, return the raw local file system, such calls to flush()
    // actually flushes the stream.
    try {
      FileSystem fs = path.getFileSystem(conf);
      if (fs instanceof LocalFileSystem) {
        LOG.debug("{} is local file system", path);
        return ((LocalFileSystem) fs).getRawFileSystem();
      }
      return fs;
    } catch (IOException e) {
      LOG.error("Fail to get filesystem of {}", path);
      throw e;
    }
  }

  public static Map<String, Map<String, String>> extractRemoteStorageConf(String confString) {
    Map<String, Map<String, String>> res = Maps.newHashMap();
    if (StringUtils.isEmpty(confString)) {
      return res;
    }

    String[] clusterConfItems = confString.split(Constants.SEMICOLON_SPLIT_CHAR);
    String msg = "Cluster specific conf[{}] format[cluster,k1=v1;...] is wrong.";
    if (ArrayUtils.isEmpty(clusterConfItems)) {
      LOG.warn(msg, confString);
      return res;
    }

    for (String s : clusterConfItems) {
      String[] item = s.split(Constants.COMMA_SPLIT_CHAR);
      if (ArrayUtils.isEmpty(item) || item.length < 2) {
        LOG.warn(msg, s);
        return Maps.newHashMap();
      }

      String clusterId = item[0];
      Map<String, String> curClusterConf = Maps.newHashMap();
      for (int i = 1; i < item.length; ++i) {
        String[] kv = item[i].split(Constants.EQUAL_SPLIT_CHAR);
        if (ArrayUtils.isEmpty(item) || kv.length != 2) {
          LOG.warn(msg, s);
          return Maps.newHashMap();
        }
        String key = kv[0].trim();
        String value = kv[1].trim();
        if (StringUtils.isEmpty((key)) || StringUtils.isEmpty(value)) {
          LOG.warn("This cluster conf[{}] format is wrong[k=v]", s);
          return Maps.newHashMap();
        }
        curClusterConf.put(key, value);
      }
      res.put(clusterId, curClusterConf);
    }
    return res;
  }
}
