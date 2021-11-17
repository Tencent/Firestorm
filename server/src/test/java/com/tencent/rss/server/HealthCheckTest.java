/*
 * Tencent is pleased to support the open source community by making
 * Firestorm-Spark remote shuffle server available.
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

import org.junit.Test;

import java.io.File;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class HealthCheckTest {

  private int mode = 0;

  @Test
  public void constructorTest() {
    ShuffleServerConf conf = new ShuffleServerConf();
    assertConf(conf);
    conf.setString(ShuffleServerConf.RSS_HEALTH_CHECKERS, "");
    assertConf(conf);
    conf.setString(ShuffleServerConf.RSS_HEALTH_CHECKERS, "com.tencent.rss.server.StorageChecker");
    conf.set(ShuffleServerConf.RSS_STORAGE_BASE_PATH, "s1");
    conf.set(ShuffleServerConf.RSS_HEALTH_MIN_STORAGE_PERCENTAGE, -1.0);
    assertConf(conf);
    conf.set(ShuffleServerConf.RSS_HEALTH_MIN_STORAGE_PERCENTAGE, 102.0);
    assertConf(conf);
    conf.set(ShuffleServerConf.RSS_HEALTH_MIN_STORAGE_PERCENTAGE, 1.0);
    conf.set(ShuffleServerConf.RSS_HEALTH_CHECK_INTERVAL, -1L);
    assertConf(conf);
    conf.set(ShuffleServerConf.RSS_HEALTH_CHECK_INTERVAL, 1L);
    conf.set(ShuffleServerConf.RSS_HEALTH_STORAGE_MAX_USAGE_PERCENTAGE, -1.0);
    assertConf(conf);
    conf.set(ShuffleServerConf.RSS_HEALTH_STORAGE_MAX_USAGE_PERCENTAGE, 101.0);
    assertConf(conf);
    conf.set(ShuffleServerConf.RSS_HEALTH_STORAGE_MAX_USAGE_PERCENTAGE, 1.0);
    conf.set(ShuffleServerConf.RSS_HEALTH_STORAGE_RECOVERY_USAGE_PERCENTAGE, -1.0);
    assertConf(conf);
    conf.set(ShuffleServerConf.RSS_HEALTH_STORAGE_RECOVERY_USAGE_PERCENTAGE, 101.0);
    assertConf(conf);
    conf.set(ShuffleServerConf.RSS_HEALTH_STORAGE_RECOVERY_USAGE_PERCENTAGE, 1.0);
    new HealthCheck(new AtomicBoolean(), conf);
  }

  private void assertConf(ShuffleServerConf conf) {
    boolean isThrown;
    isThrown = false;
    try {
      new HealthCheck(new AtomicBoolean(), conf);
    } catch (IllegalArgumentException e) {
      isThrown = true;
    }
    assertTrue(isThrown);
  }

  @Test
  public void checkTest() throws Exception {
    ShuffleServerConf conf = new ShuffleServerConf();
    conf.set(ShuffleServerConf.RSS_STORAGE_BASE_PATH, "st1,st2,st3");
    conf.set(ShuffleServerConf.RSS_HEALTH_MIN_STORAGE_PERCENTAGE, 55.0);
    StorageChecker checker = new MockStorageChecker(conf);

    assertTrue(checker.checkIsHealthy());

    mode++;
    assertTrue(checker.checkIsHealthy());

    mode++;
    assertFalse(checker.checkIsHealthy());

    mode++;
    assertTrue(checker.checkIsHealthy());
    conf.set(ShuffleServerConf.RSS_HEALTH_MIN_STORAGE_PERCENTAGE, 80.0);
    checker = new MockStorageChecker(conf);
    assertFalse(checker.checkIsHealthy());

    mode++;
    checker.checkIsHealthy();
    assertTrue(checker.checkIsHealthy());
  }

  private class MockStorageChecker extends StorageChecker {
    public MockStorageChecker(ShuffleServerConf conf) {
      super(conf);
    }

    @Override
    long getTotalSpace(File file) {
      return 1000;
    }

    @Override
    long getUsedSpace(File file) {
      long result = 0;
      switch (file.getPath()) {
        case "st1":
          switch (mode) {
            case 0:
              result = 100;
              break;
            case 1:
            case 2:
            case 3:
              result = 900;
              break;
            case 4:
              result = 150;
              break;
            default:
              break;
          }
          break;
        case "st2":
          switch (mode) {
            case 0:
            case 1:
              result = 200;
              break;
            case 2:
              result = 900;
              break;
            case 3:
              result = 400;
              break;
            case 4:
              result = 100;
            default:
              break;
          }
          break;
        case "st3":
          switch (mode) {
            case 0:
            case 1:
            case 2:
            case 3:
              result = 300;
              break;
            default:
              break;
          }
          break;
      }
      return result;
    }
  }
}