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

package com.tencent.rss.server;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Uninterruptibles;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * HealthCheck will check every server whether has the ability to process shuffle data. Currently, we only support disk
 * checker. If enough disks don't have enough disk space, server will become unhealthy, and only enough disks
 * have enough disk space, server will become healthy again.
 **/
public class HealthCheck {

  private static final Logger LOG = LoggerFactory.getLogger(HealthCheck.class);

  private final AtomicBoolean isHealthy;
  private final double diskMaxUsagePercentage;
  private final double diskRecoveryUsagePercentage;
  private final long checkIntervalMs;
  private final double minStorageHealthyPercentage;
  private final Thread thread;
  private final List<StorageInfo> storageInfos  = Lists.newArrayList();

  public HealthCheck(AtomicBoolean isHealthy, ShuffleServerConf conf) {
    this.isHealthy = isHealthy;
    String[] storagePaths = conf.get(ShuffleServerConf.RSS_STORAGE_BASE_PATH).split(",");
    for (String path : storagePaths) {
      storageInfos.add(new StorageInfo(path));
    }
    this.diskMaxUsagePercentage = conf.getDouble(ShuffleServerConf.RSS_STORAGE_MAX_USAGE_PERCENTAGE);
    this.diskRecoveryUsagePercentage = conf.getDouble(ShuffleServerConf.RSS_STORAGE_RECOVERY_USAGE_PERCENTAGE);
    this.checkIntervalMs = conf.getLong(ShuffleServerConf.RSS_HEALTH_CHECK_INTERVAL);
    this.minStorageHealthyPercentage = conf.getDouble(ShuffleServerConf.RSS_MIN_STORAGE_HEALTHY_PERCENTAGE);
    this.thread = new Thread(() -> {
      try {
        check();
        Uninterruptibles.sleepUninterruptibly(checkIntervalMs, TimeUnit.MICROSECONDS);
      } catch (Exception e) {
        LOG.error(e.getMessage());
      }
    });
    thread.setName("HealthCheckService");
    thread.setDaemon(true);
  }

  public void check() {
    int num = 0;
    for (StorageInfo storageInfo : storageInfos) {
      if (storageInfo.checkIsHealthy()) {
        num++;
      }
    }

    if (storageInfos.isEmpty()) {
      isHealthy.set(false);
      return;
    }

    double availablePercentage = num * 100.0 / storageInfos.size();
    if (Double.compare(availablePercentage, minStorageHealthyPercentage) >= 0) {
      isHealthy.set(true);
    } else {
      isHealthy.set(false);
    }
  }

  public void start() {
    thread.start();
  }

  public void stop() throws InterruptedException {
    thread.join();
  }

  // todo: This function will be integrated to MultiStorageManager, currently we only support disk check.
  class StorageInfo {

    private final File storageDir;
    private boolean isHealthy;

    StorageInfo(String path) {
      this.storageDir = new File(path);
      this.isHealthy = true;
    }

    boolean checkIsHealthy() {
      if (Double.compare(0.0, storageDir.getTotalSpace()) == 0) {
        this.isHealthy = false;
        return false;
      }
      double usagePercent = storageDir.getUsableSpace() * 100.0 / storageDir.getTotalSpace();
      if (isHealthy) {
        if (Double.compare(usagePercent, diskMaxUsagePercentage) >= 0) {
          isHealthy = false;
        }
      } else {
        if (Double.compare(usagePercent, diskRecoveryUsagePercentage) <= 0) {
          isHealthy = true;
        }
      }
      return isHealthy;
    }
  }
}
