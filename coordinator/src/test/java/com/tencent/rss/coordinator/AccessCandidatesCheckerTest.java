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

import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.Map;
import java.util.Objects;

import com.google.common.collect.Sets;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static java.lang.Thread.sleep;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class AccessCandidatesCheckerTest {
  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  @Before
  public void setUp() {
    CoordinatorMetrics.register();
  }

  @Test
  public void testHandleAccessRequest() {
    try {
      File cfgFile = folder.newFile();
      FileWriter fileWriter = new FileWriter(cfgFile);
      PrintWriter printWriter = new PrintWriter(fileWriter);
      printWriter.println("9527");
      printWriter.println(" 135 ");
      printWriter.println("2 ");
      printWriter.flush();
      printWriter.close();

      final String filePath = Objects.requireNonNull(
          getClass().getClassLoader().getResource("coordinator.conf")).getFile();
      CoordinatorConf conf = new CoordinatorConf(filePath);
      conf.set(CoordinatorConf.COORDINATOR_ACCESS_CANDIDATES_PATH, cfgFile.getAbsolutePath());
      conf.setString(CoordinatorConf.COORDINATOR_ACCESS_CHECKERS,
          "com.tencent.rss.coordinator.AccessCandidatesChecker");
      conf.setInteger(CoordinatorConf.COORDINATOR_ACCESS_CLEANUP_INTERVAL_SEC, 1);
      AccessManager accessManager = new AccessManager(conf, null);
      AccessCandidatesChecker checker = (AccessCandidatesChecker) accessManager.getAccessCheckers().get(0);
      sleep(1200);
      assertEquals(Sets.newHashSet("2", "9527", "135"), checker.getCandidates().get());
      assertTrue(checker.check("9527_1").isSuccess());
      accessManager.getAccessedCronTaskParams().put("9527_1", System.currentTimeMillis() / 1000);
      assertTrue(checker.check("135_1").isSuccess());
      accessManager.getAccessedCronTaskParams().put("135_1", System.currentTimeMillis() / 1000);
      assertTrue(checker.check("135_2").isSuccess());
      accessManager.getAccessedCronTaskParams().put("135_2", System.currentTimeMillis() / 1000);
      assertFalse(checker.check("1").isSuccess());
      assertFalse(checker.check("1_2").isSuccess());

      Map<String, Long> taskParams = accessManager.getAccessedCronTaskParams();
      assertEquals(3, taskParams.size());
      assertTrue(taskParams.containsKey("9527_1"));
      assertTrue(taskParams.containsKey("135_1"));
      assertTrue(taskParams.containsKey("135_2"));

      sleep(1100);
      fileWriter = new FileWriter(cfgFile);
      printWriter = new PrintWriter(fileWriter);
      printWriter.println("13");
      printWriter.println("57");
      printWriter.close();

      sleep(1200);
      assertEquals(Sets.newHashSet("13", "57"), checker.getCandidates().get());
      assertTrue(checker.check("13_1").isSuccess());
      accessManager.getAccessedCronTaskParams().put("13_1", System.currentTimeMillis() / 1000);
      assertTrue(checker.check("57_1").isSuccess());
      accessManager.getAccessedCronTaskParams().put("57_1", System.currentTimeMillis() / 1000);
      assertTrue(checker.check("57_2").isSuccess());
      accessManager.getAccessedCronTaskParams().put("57_2", System.currentTimeMillis() / 1000);
      taskParams = accessManager.getAccessedCronTaskParams();
      assertEquals(3, taskParams.size());
      assertTrue(taskParams.containsKey("13_1"));
      assertTrue(taskParams.containsKey("57_1"));
      assertTrue(taskParams.containsKey("57_2"));

      checker.stop();
      folder.delete();
    } catch (Exception e) {
      fail(e.getMessage());
    }
  }
}
