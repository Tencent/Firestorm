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

package org.apache.spark.shuffle;

import java.util.List;
import java.util.NoSuchElementException;

import com.google.common.collect.Lists;
import org.apache.spark.SparkConf;
import org.apache.spark.shuffle.sort.SortShuffleManager;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import com.tencent.rss.client.api.CoordinatorClient;
import com.tencent.rss.client.response.RssAccessClusterResponse;

import static com.tencent.rss.client.response.ResponseStatusCode.ACCESS_DENIED;
import static com.tencent.rss.client.response.ResponseStatusCode.SUCCESS;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

public class DelegationRssShuffleManagerTest {
  private static MockedStatic<RssShuffleUtils> mockedStaticRssShuffleUtils;

  @BeforeClass
  public static void setUp() {
    mockedStaticRssShuffleUtils = mockStatic(RssShuffleUtils.class, Mockito.CALLS_REAL_METHODS);
  }

  @AfterClass
  public static void tearDown() {
    mockedStaticRssShuffleUtils.close();
  }

  @Test
  public void testCreateInDriverDenied() throws Exception {
    CoordinatorClient mockCoordinatorClient = mock(CoordinatorClient.class);
    when(mockCoordinatorClient.accessCluster(any())).thenReturn(new RssAccessClusterResponse(ACCESS_DENIED, ""));
    List<CoordinatorClient> coordinatorClients = Lists.newArrayList();
    coordinatorClients.add(mockCoordinatorClient);
    mockedStaticRssShuffleUtils.when(() ->
        RssShuffleUtils.createCoordinatorClients(any())).thenReturn(coordinatorClients);
    SparkConf conf = new SparkConf();
    assertCreateSortShuffleManager(conf);
  }

  @Test
  public void testCreateInDriver() throws Exception {
    CoordinatorClient mockCoordinatorClient = mock(CoordinatorClient.class);
    when(mockCoordinatorClient.accessCluster(any())).thenReturn(new RssAccessClusterResponse(SUCCESS, ""));
    List<CoordinatorClient> coordinatorClients = Lists.newArrayList();
    coordinatorClients.add(mockCoordinatorClient);
    mockedStaticRssShuffleUtils.when(() ->
        RssShuffleUtils.createCoordinatorClients(any())).thenReturn(coordinatorClients);

    SparkConf conf = new SparkConf();
    assertCreateSortShuffleManager(conf);

    conf.set(RssClientConfig.RSS_ACCESS_ID, "mockId");
    assertCreateSortShuffleManager(conf);

    conf.set(RssClientConfig.RSS_COORDINATOR_QUORUM, "m1:8001,m2:8002");
    assertCreateRssShuffleManager(conf);
  }

  @Test
  public void testCreateInExecutor() throws Exception {
    DelegationRssShuffleManager delegationRssShuffleManager;
    SparkConf conf = new SparkConf();
    conf.set(RssClientConfig.RSS_COORDINATOR_QUORUM, "m1:8001,m2:8002");
    delegationRssShuffleManager = new DelegationRssShuffleManager(conf, false);
    assertFalse(delegationRssShuffleManager.getDelegate() instanceof RssShuffleManager);
    assertTrue(delegationRssShuffleManager.getDelegate() instanceof SortShuffleManager);
  }

  @Test
  public void testCreateFallback() throws Exception {
    CoordinatorClient mockCoordinatorClient = mock(CoordinatorClient.class);
    when(mockCoordinatorClient.accessCluster(any())).thenReturn(new RssAccessClusterResponse(SUCCESS, ""));
    List<CoordinatorClient> coordinatorClients = Lists.newArrayList();
    coordinatorClients.add(mockCoordinatorClient);
    mockedStaticRssShuffleUtils.when(() ->
        RssShuffleUtils.createCoordinatorClients(any())).thenReturn(coordinatorClients);
    DelegationRssShuffleManager delegationRssShuffleManager;

    SparkConf conf = new SparkConf();
    conf.set(RssClientConfig.RSS_ACCESS_ID, "mockId");
    conf.set(RssClientConfig.RSS_USE_RSS_SHUFFLE_MANAGER, "true");

    // fall back to SortShuffleManager in driver
    assertCreateSortShuffleManager(conf);

    // No fall back in executor
    conf.set(RssClientConfig.RSS_USE_RSS_SHUFFLE_MANAGER, "true");
    boolean hasException = false;
    try {
      new DelegationRssShuffleManager(conf, false);
    } catch (NoSuchElementException e) {
      assertTrue(e.getMessage().startsWith("spark.rss.coordinator.quorum"));
      hasException = true;
    }
    assertTrue(hasException);
  }

  private DelegationRssShuffleManager assertCreateSortShuffleManager(SparkConf conf) throws Exception {
    DelegationRssShuffleManager delegationRssShuffleManager = new DelegationRssShuffleManager(conf, true);
    assertTrue(delegationRssShuffleManager.getDelegate() instanceof SortShuffleManager);
    assertFalse(delegationRssShuffleManager.getDelegate() instanceof RssShuffleManager);
    assertFalse(Boolean.parseBoolean(conf.get(RssClientConfig.RSS_USE_RSS_SHUFFLE_MANAGER)));
    return delegationRssShuffleManager;
  }

  private DelegationRssShuffleManager assertCreateRssShuffleManager(SparkConf conf) throws Exception {
    DelegationRssShuffleManager delegationRssShuffleManager = new DelegationRssShuffleManager(conf, true);
    assertFalse(delegationRssShuffleManager.getDelegate() instanceof SortShuffleManager);
    assertTrue(delegationRssShuffleManager.getDelegate() instanceof RssShuffleManager);
    assertTrue(Boolean.parseBoolean(conf.get(RssClientConfig.RSS_USE_RSS_SHUFFLE_MANAGER)));
    return delegationRssShuffleManager;
  }
}
