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

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tencent.rss.common.util.RssUtils;

public class AccessManager {

  private static final Logger LOG = LoggerFactory.getLogger(AccessManager.class);

  private final int cleanUpIntervalS;
  private final long accessExpireThresholdS;
  private final CoordinatorConf coordinatorConf;
  private final ClusterManager clusterManager;
  private List<AccessChecker> accessCheckers = Lists.newArrayList();
  private final Map<String, Long> accessedCronTaskParams = Maps.newConcurrentMap();

  private ScheduledExecutorService cleanUpExpiredTaskParamsSES = null;

  public AccessManager(CoordinatorConf conf, ClusterManager clusterManager) throws RuntimeException {
    this.coordinatorConf = conf;
    this.clusterManager = clusterManager;
    this.cleanUpIntervalS = conf.get(CoordinatorConf.COORDINATOR_ACCESS_CLEANUP_INTERVAL_SEC);
    this.accessExpireThresholdS = conf.get(CoordinatorConf.COORDINATOR_ACCESS_EXPIRE_THRESHOLD_SEC);
    init();
  }

  private void init() throws RuntimeException {
    String checkers = coordinatorConf.get(CoordinatorConf.COORDINATOR_ACCESS_CHECKERS);
    if (StringUtils.isEmpty(checkers)) {
      String msg = "Access checkers can not be empty once access manager enabled.";
      LOG.error(msg);
      throw new RuntimeException(msg);
    }

    String[] names = checkers.trim().split(",");
    if (ArrayUtils.isEmpty(names)) {
      String msg = String.format("%s is wrong and empty access checkers, AccessManager will do nothing.",
          CoordinatorConf.COORDINATOR_ACCESS_CHECKERS.toString());
      throw new RuntimeException(msg);
    }

    accessCheckers = RssUtils.loadExtensions(AccessChecker.class, Arrays.asList(names), this);
    if (accessCheckers.isEmpty()) {
      String msg = String.format("%s is wrong and empty access checkers, AccessManager will do nothing.",
          CoordinatorConf.COORDINATOR_ACCESS_CHECKERS.toString());
      throw new RuntimeException(msg);
    }

    cleanUpExpiredTaskParamsSES = Executors.newSingleThreadScheduledExecutor(
        new ThreadFactoryBuilder().setDaemon(true).setNameFormat("CleanUpExpiredTaskParams-%d").build());
    cleanUpExpiredTaskParamsSES.scheduleAtFixedRate(
        this::cleanUpExpiredAccessedTaskParams, 0, cleanUpIntervalS, TimeUnit.SECONDS);
  }

  public AccessCheckResult handleAccessRequest(String cronTaskParam) {
    if (accessedCronTaskParams.containsKey(cronTaskParam)) {
      String msg = String.format("Reject task[%s] for it is a retrying task.", cronTaskParam);
      LOG.info(msg);
      return new AccessCheckResult(false, msg);
    }

    for (AccessChecker checker : accessCheckers) {
      AccessCheckResult accessCheckResult = checker.check(cronTaskParam);
      if (!accessCheckResult.isSuccess()) {
        return accessCheckResult;
      }
    }

    accessedCronTaskParams.put(cronTaskParam, System.currentTimeMillis() / 1000);
    return new AccessCheckResult(true, "");
  }

  private void cleanUpExpiredAccessedTaskParams() {
    List<String> expiredTaskParams = Lists.newArrayList();
    for (Map.Entry<String, Long> entry : accessedCronTaskParams.entrySet()) {
      long ts = entry.getValue();
      long cur = (System.currentTimeMillis() - ts) / 1000;
      if (cur > accessExpireThresholdS) {
        expiredTaskParams.add(entry.getKey());
      }
    }

    for (String taskParam : expiredTaskParams) {
      accessedCronTaskParams.remove(taskParam);
    }
  }

  public CoordinatorConf getCoordinatorConf() {
    return coordinatorConf;
  }

  public ClusterManager getClusterManager() {
    return clusterManager;
  }

  public Map<String, Long> getAccessedCronTaskParams() {
    return accessedCronTaskParams;
  }

  public List<AccessChecker> getAccessCheckers() {
    return accessCheckers;
  }

  public void close() {
    for (AccessChecker checker : accessCheckers) {
      checker.stop();
    }

    if (cleanUpExpiredTaskParamsSES != null) {
      cleanUpExpiredTaskParamsSES.shutdownNow();
    }
  }
}
