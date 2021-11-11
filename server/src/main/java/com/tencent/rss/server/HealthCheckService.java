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

import java.io.File;
import java.util.concurrent.atomic.AtomicBoolean;

public class HealthCheckService {

  private final AtomicBoolean isHealthy;
  private boolean isDiskSpaceHealthy = true;
  private Thread thread;
  private String[] storagePaths;

  public HealthCheckService(AtomicBoolean isHealthy, ShuffleServerConf conf) {
    this.isHealthy = isHealthy;
    this.storagePaths = conf.get(ShuffleServerConf.RSS_STORAGE_BASE_PATH).split(",");
  }

  public void check() {
    boolean allDiskFull = false;
    for (String path : storagePaths) {
      File file = new File(path);
      double usagePercent = file.getUsableSpace() * 100.0 / file.getTotalSpace();

    }
    if (isDiskSpaceHealthy) {
      isHealthy.set(false);
    } else {
      isHealthy.set(true);
    }
  }

  public void start() {
    thread.start();
  }

  public void stop() {
    thread.join();
  }
}
