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

import java.util.Map;

import com.google.common.collect.Maps;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;

import com.tencent.rss.common.metrics.MetricsManager;

public class CoordinatorMetrics {

  private static final String TOTAL_SERVER_NUM = "total_server_num";
  private static final String RUNNING_APP_NUM = "running_app_num";
  private static final String TOTAL_APP_NUM = "total_app_num";
  private static final String EXCLUDE_SERVER_NUM = "exclude_server_num";
  private static final String TOTAL_ACCESS_REQUEST = "total_access_request";
  private static final String TOTAL_CANDIDATES_DENIED_REQUEST = "total_candidates_denied_request";
  private static final String TOTAL_LOAD_DENIED_REQUEST = "total_load_denied_request";
  public static final String REMOTE_STORAGE_IN_USED_PREFIX = "remote_storage_";

  static Gauge gaugeTotalServerNum;
  static Gauge gaugeExcludeServerNum;
  static Gauge gaugeRunningAppNum;
  static Counter counterTotalAppNum;
  static Counter counterTotalAccessRequest;
  static Counter counterTotalCandidatesDeniedRequest;
  static Counter counterTotalLoadDeniedRequest;
  static Map<String, Gauge> gaugeInUsedRemoteStorage = Maps.newConcurrentMap();

  private static MetricsManager metricsManager;
  private static boolean isRegister = false;

  public static synchronized void register(CollectorRegistry collectorRegistry) {
    if (!isRegister) {
      metricsManager = new MetricsManager(collectorRegistry);
      isRegister = true;
      setUpMetrics();
    }
  }

  public static void register() {
    register(CollectorRegistry.defaultRegistry);
  }

  public static CollectorRegistry getCollectorRegistry() {
    return metricsManager.getCollectorRegistry();
  }

  public static Gauge addDynamicGauge(String metricsName) {
    return metricsManager.addGauge(metricsName);
  }

  private static void setUpMetrics() {
    gaugeTotalServerNum = metricsManager.addGauge(TOTAL_SERVER_NUM);
    gaugeExcludeServerNum = metricsManager.addGauge(EXCLUDE_SERVER_NUM);
    gaugeRunningAppNum = metricsManager.addGauge(RUNNING_APP_NUM);
    counterTotalAppNum = metricsManager.addCounter(TOTAL_APP_NUM);
    counterTotalAccessRequest = metricsManager.addCounter(TOTAL_ACCESS_REQUEST);
    counterTotalCandidatesDeniedRequest = metricsManager.addCounter(TOTAL_CANDIDATES_DENIED_REQUEST);
    counterTotalLoadDeniedRequest = metricsManager.addCounter(TOTAL_LOAD_DENIED_REQUEST);
  }
}
