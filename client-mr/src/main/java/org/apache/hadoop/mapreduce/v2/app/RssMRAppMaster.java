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
package org.apache.hadoop.mapreduce.v2.app;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.tencent.rss.client.api.ShuffleWriteClient;
import com.tencent.rss.client.factory.ShuffleClientFactory;
import com.tencent.rss.common.PartitionRange;
import com.tencent.rss.common.ShuffleAssignmentsInfo;
import com.tencent.rss.common.ShuffleServerInfo;
import com.tencent.rss.common.util.Constants;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Cluster;
import org.apache.hadoop.mapreduce.JobSubmissionFiles;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class RssMRAppMaster {
  private static final Logger LOG = LoggerFactory.getLogger(RssMRAppMaster.class);

  public static final String RSS_CLIENT_HEARTBEAT_THREAD_NUM = "mapreduce.rss.client.heartBeat.threadNum";
  public static final int RSS_CLIENT_HEARTBEAT_THREAD_NUM_DEFAULT_VALUE = 4;
  public static final String RSS_CLIENT_TYPE = "mapreduce.rss.client.type";
  public static final String RSS_CLIENT_TYPE_DEFAULT_VALUE = "GRPC";
  public static final String RSS_CLIENT_RETRY_MAX = "mapreduce.rss.client.retry.max";
  public static final int RSS_CLIENT_RETRY_MAX_DEFAULT_VALUE = 100;
  public static final String RSS_CLIENT_RETRY_INTERVAL_MAX = "mapreduce.rss.client.retry.interval.max";
  public static final long RSS_CLIENT_RETRY_INTERVAL_MAX_DEFAULT_VALUE = 10000;
  public static final String RSS_COORDINATOR_QUORUM = "mapreduce.rss.coordinator.quorum";
  public static final String RSS_DATA_REPLICA = "mapreduce.rss.data.replica";
  public static final int RSS_DATA_REPLICA_DEFAULT_VALUE = 1;
  public static final String RSS_HEARTBEAT_INTERVAL = "mapreduce.rss.heartbeat.interval";
  public static final long RSS_HEARTBEAT_INTERVAL_DEFAULT_VALUE = 10 * 1000L;
  public static final String RSS_HEARTBEAT_TIMEOUT = "mapreduce.rss.heartbeat.timeout";
  public static final String RSS_ASSIGNMENT_PREFIX = "mapreduce.rss.assignment.partition.";

  public static void main(String[] args) {

    String containerIdStr =
        System.getenv(ApplicationConstants.Environment.CONTAINER_ID.name());
    ContainerId containerId = ContainerId.fromString(containerIdStr);
    ApplicationAttemptId applicationAttemptId =
        containerId.getApplicationAttemptId();
    JobConf conf = new JobConf(new YarnConfiguration());
    conf.addResource(new Path(MRJobConfig.JOB_CONF_FILE));
    int numReduceTasks = conf.getInt(MRJobConfig.NUM_REDUCES, 0);
    String clientType = conf.get(RSS_CLIENT_TYPE, RSS_CLIENT_TYPE_DEFAULT_VALUE);
    int heartBeatThreadNum = conf.getInt(RSS_CLIENT_HEARTBEAT_THREAD_NUM, RSS_CLIENT_HEARTBEAT_THREAD_NUM_DEFAULT_VALUE);
    int retryMax = conf.getInt(RSS_CLIENT_RETRY_MAX, RSS_CLIENT_RETRY_MAX_DEFAULT_VALUE);
    long retryIntervalMax = conf.getLong(RSS_CLIENT_RETRY_INTERVAL_MAX, RSS_CLIENT_RETRY_INTERVAL_MAX_DEFAULT_VALUE);
    String coordinators = conf.get(RSS_COORDINATOR_QUORUM);
    long heartbeatInterval = conf.getLong(RSS_HEARTBEAT_INTERVAL, RSS_HEARTBEAT_INTERVAL_DEFAULT_VALUE);
    long heartbeatTimeout = conf.getLong(RSS_HEARTBEAT_TIMEOUT, heartbeatInterval / 2);

    ShuffleWriteClient client = ShuffleClientFactory
        .getInstance()
        .createShuffleWriteClient(clientType, retryMax, retryIntervalMax, heartBeatThreadNum);

    LOG.info("Registering coordinators {}", coordinators);
    client.registerCoordinators(coordinators);
    int dataReplica = conf.getInt(RSS_DATA_REPLICA, RSS_DATA_REPLICA_DEFAULT_VALUE);
    // get all register info according to coordinator's response
    ShuffleAssignmentsInfo response = client.getShuffleAssignments(
        applicationAttemptId.toString(), 0, numReduceTasks,
        1, dataReplica, Sets.newHashSet(Constants.SHUFFLE_SERVER_VERSION));
    Map<ShuffleServerInfo, List<PartitionRange>> serverToPartitionRanges = response.getServerToPartitionRanges();
    final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
    if (serverToPartitionRanges == null || serverToPartitionRanges.isEmpty()) {
      return;
    }
    LOG.info("Start to register shuffle");
    long start = System.currentTimeMillis();
    serverToPartitionRanges.entrySet().forEach(entry -> {
      client.registerShuffle(
          entry.getKey(), applicationAttemptId.toString(), 0, entry.getValue());
    });
    LOG.info("Finish register shuffle with " + (System.currentTimeMillis() - start) + " ms");
    scheduledExecutorService.scheduleAtFixedRate(
        () -> {
          try {
            client.sendAppHeartbeat(applicationAttemptId.toString(), heartbeatTimeout);
            LOG.info("Finish send heartbeat to coordinator and servers");
          } catch (Exception e) {
            LOG.warn("Fail to send heartbeat to coordinator and servers", e);
          }
        },
        heartbeatInterval / 2,
        heartbeatInterval,
        TimeUnit.MILLISECONDS);

    // write shuffle worker assignments to submit work directory
    // format is as below:
    // mapreduce.rss.assignment.partition.1:server1,server2
    // mapreduce.rss.assignment.partition.2:server3,server4
    // ...

    response.getPartitionToServers().entrySet().forEach(entry -> {
      List<String> servers = Lists.newArrayList();
      for (ShuffleServerInfo server : entry.getValue()) {
        servers.add(server.getHost() + ":" + server.getPort());
      }
      conf.set(RSS_ASSIGNMENT_PREFIX + entry.getKey(), StringUtils.join(servers, ","));
    });
    String jobDirStr = conf.get(MRJobConfig.MAPREDUCE_JOB_DIR);
    if (jobDirStr == null) {
      throw new RuntimeException("");
    }
    Path newJobConfFile = new Path(jobDirStr, MRJobConfig.JOB_CONF_FILE + ".bak");
    Path oldJobConfFile = new Path(jobDirStr, MRJobConfig.JOB_CONF_FILE);
    try {
      FileSystem fs = new Cluster(conf).getFileSystem();
      try (FSDataOutputStream out =
             FileSystem.create(fs, newJobConfFile,
                 new FsPermission(JobSubmissionFiles.JOB_FILE_PERMISSION))) {
          conf.writeXml(out);
          fs.delete(oldJobConfFile, true);
          fs.rename(newJobConfFile, oldJobConfFile);
      }
    } catch (Exception e) {
      LOG.error("Modify job conf exception", e);
      throw new RuntimeException("Modify job conf exception ", e);
    }
    MRAppMaster.main(args);
    scheduledExecutorService.shutdown();
  }
}
