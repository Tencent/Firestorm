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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.Lists;
import java.util.List;
import java.util.Objects;
import org.junit.Test;

public class AccessClusterLoadCheckerTest {

  @Test
  public void test() {
    try {
      ClusterManager clusterManager = mock(SimpleClusterManager.class);
      List<ServerNode> serverNodeList = Lists.newArrayList();
      ServerNode node1 = new ServerNode(
          "1",
          "1",
          0,
          50,
          20,
          30,
          0,
          null,
          false);
      serverNodeList.add(node1);

      final String filePath = Objects.requireNonNull(
          getClass().getClassLoader().getResource("coordinator.conf")).getFile();
      CoordinatorConf conf = new CoordinatorConf(filePath);
      conf.setString(CoordinatorConf.COORDINATOR_ACCESS_CHECKERS,
          "com.tencent.rss.coordinator.AccessClusterLoadChecker");
      AccessManager accessManager = new AccessManager(conf, clusterManager);
      AccessClusterLoadChecker accessClusterLoadChecker =
          (AccessClusterLoadChecker) accessManager.getAccessCheckers().get(0);
      when(clusterManager.getServerList()).thenReturn(serverNodeList);
      assertFalse(accessClusterLoadChecker.check("test").isSuccess());
      assertEquals(2, accessClusterLoadChecker.getAvailableServerNumThreshold());
      assertEquals(0, Double.compare(accessClusterLoadChecker.getMemoryPercentThreshold(), 20.0));

      ServerNode node2 = new ServerNode(
          "1",
          "1",
          0,
          50,
          40,
          10,
          0,
          null,
          true);
      serverNodeList.add(node2);
      ServerNode node3 = new ServerNode(
          "1",
          "1",
          0,
          50,
          25,
          25,
          0,
          null,
          true);
      serverNodeList.add(node3);
      assertFalse(accessClusterLoadChecker.check("test").isSuccess());
      ServerNode node4 = new ServerNode(
          "1",
          "1",
          0,
          50,
          25,
          25,
          0,
          null,
          true);
      serverNodeList.add(node4);
      assertTrue(accessClusterLoadChecker.check("test").isSuccess());
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }
}
