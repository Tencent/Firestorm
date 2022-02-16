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

package com.tencent.rss.test;

import com.google.common.util.concurrent.Uninterruptibles;
import com.tencent.rss.coordinator.CoordinatorConf;
import org.apache.spark.SparkConf;
import org.apache.spark.shuffle.RssClientConfig;
import org.apache.spark.shuffle.RssShuffleManager;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class DynamicFetchClientConfTest extends IntegrationTestBase {
  @Rule
  public TemporaryFolder tmpFolder = new TemporaryFolder();

  @Test
  public void test() throws Exception {
    SparkConf sparkConf = new SparkConf();
    sparkConf.set("spark.shuffle.manager", "org.apache.spark.shuffle.RssShuffleManager");
    sparkConf.set(RssClientConfig.RSS_COORDINATOR_QUORUM, COORDINATOR_QUORUM);
    sparkConf.set("spark.mock.2", "no-overwrite-conf");
    sparkConf.set("spark.shuffle.service.enabled", "true");

    File clientConfFile = tmpFolder.newFile();
    FileWriter fileWriter = new FileWriter(clientConfFile);
    PrintWriter printWriter = new PrintWriter(fileWriter);
    printWriter.println("spark.mock.1  1234");
    printWriter.println(" spark.mock.2 overwrite-conf ");
    printWriter.println(" spark.mock.3 true ");
    for (String k : RssClientConfig.RSS_MANDATORY_CLUSTER_CONF) {
      sparkConf.set(k, "Dummy-" + k);
      sparkConf.set(k, "Dummy-" + k);
      printWriter.println(k + " " + k + "-mandatory-value");
      printWriter.println(" spark.mock.3 true ");
    }
    printWriter.flush();
    printWriter.close();

    CoordinatorConf coordinatorConf = getCoordinatorConf();
    coordinatorConf.setBoolean("rss.coordinator.dynamicClientConf.enabled", true);
    coordinatorConf.setString("rss.coordinator.dynamicClientConf.path", clientConfFile.getAbsolutePath());
    coordinatorConf.setInteger("rss.coordinator.dynamicClientConf.updateIntervalSec", 10);
    createCoordinatorServer(coordinatorConf);
    startServers();

    Uninterruptibles.sleepUninterruptibly(3, TimeUnit.SECONDS);

    assertFalse(sparkConf.contains("spark.mock.1"));
    assertEquals("no-overwrite-conf", sparkConf.get("spark.mock.2"));
    assertFalse(sparkConf.contains("spark.mock.3"));
    for (String k : RssClientConfig.RSS_MANDATORY_CLUSTER_CONF) {
      assertEquals("Dummy-" + k, sparkConf.get(k));
    }
    assertTrue(sparkConf.getBoolean("spark.shuffle.service.enabled", true));

    RssShuffleManager rssShuffleManager = new RssShuffleManager(sparkConf, true);

    SparkConf sparkConf1 = rssShuffleManager.getSparkConf();
    assertEquals(1234, sparkConf1.getInt("spark.mock.1", 0));
    assertEquals("no-overwrite-conf", sparkConf1.get("spark.mock.2"));
    for (String k : RssClientConfig.RSS_MANDATORY_CLUSTER_CONF) {
      assertEquals(k + "-mandatory-value", sparkConf1.get(k));
    }
    assertFalse(sparkConf1.getBoolean("spark.shuffle.service.enabled", true));
  }
}
