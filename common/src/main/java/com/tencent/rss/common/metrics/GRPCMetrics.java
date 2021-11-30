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

package com.tencent.rss.common.metrics;

import com.google.common.collect.Maps;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;

import java.util.Map;

public class GRPCMetrics {

  private static MetricsManager metricsManager;
  private static boolean isRegister = false;
  public static Map<String, Counter> counterMap = Maps.newConcurrentMap();
  public static Map<String, Gauge> gaugeMap = Maps.newConcurrentMap();
  public static Gauge gaugeGrpcOpen;
  public static Counter counterGrpcTotal;

  // for coordinator
  private static final String GRPC_GET_SHUFFLE_ASSIGNMENTS = "grpc_get_shuffle_assignments";
  private static final String GRPC_HEARTBEAT = "grpc_heartbeat";
  private static final String GRPC_GET_SHUFFLE_ASSIGNMENTS_TOTAL =
      "grpc_get_shuffle_assignments_total";
  private static final String GRPC_HEARTBEAT_TOTAL =
      "grpc_heartbeat_total";

  // for Shuffle server
  private static final String GRPC_REGISTERED_SHUFFLE = "grpc_registered_shuffle";
  private static final String GRPC_SEND_SHUFFLE_DATA = "grpc_send_shuffle_data";
  private static final String GRPC_COMMIT_SHUFFLE_TASK = "grpc_commit_shuffle_task";
  private static final String GRPC_FINISH_SHUFFLE = "grpc_finish_shuffle";
  private static final String GRPC_REQUIRE_BUFFER = "grpc_require_buffer";
  private static final String GRPC_APP_HEARTBEAT = "grpc_app_heartbeat";
  private static final String GRPC_REPORT_SHUFFLE_RESULT = "grpc_report_shuffle_result";
  private static final String GRPC_GET_SHUFFLE_RESULT = "grpc_get_shuffle_result";
  private static final String GRPC_GET_SHUFFLE_DATA = "grpc_get_shuffle_data";
  private static final String GRPC_GET_SHUFFLE_INDEX = "grpc_get_shuffle_index";

  private static final String GRPC_OPEN = "grpc_open";
  private static final String GRPC_TOTAL = "grpc_total";
  private static final String GRPC_REGISTERED_SHUFFLE_TOTAL = "grpc_registered_shuffle_total";
  private static final String GRPC_SEND_SHUFFLE_DATA_TOTAL = "grpc_send_shuffle_data_total";
  private static final String GRPC_COMMIT_SHUFFLE_TASK_TOTAL = "grpc_commit_shuffle_task_total";
  private static final String GRPC_FINISH_SHUFFLE_TOTAL = "grpc_finish_shuffle_total";
  private static final String GRPC_REQUIRE_BUFFER_TOTAL = "grpc_require_buffer_total";
  private static final String GRPC_APP_HEARTBEAT_TOTAL = "grpc_app_heartbeat_total";
  private static final String GRPC_REPORT_SHUFFLE_RESULT_TOTAL = "grpc_report_shuffle_result_total";
  private static final String GRPC_GET_SHUFFLE_RESULT_TOTAL = "grpc_get_shuffle_result_total";
  private static final String GRPC_GET_SHUFFLE_DATA_TOTAL = "grpc_get_shuffle_data_total";
  private static final String GRPC_GET_SHUFFLE_INDEX_TOTAL = "grpc_get_shuffle_index_total";

  // for coordinator
  public static final String HEARTBEAT_METHOD = "heartbeat";
  public static final String GET_SHUFFLE_ASSIGNMENTS_METHOD = "getShuffleAssignments";

  // for Shuffle server
  public static final String REGISTER_SHUFFLE_METHOD = "registerShuffle";
  public static final String SEND_SHUFFLE_DATA_METHOD = "sendShuffleData";
  public static final String COMMIT_SHUFFLE_TASK_METHOD = "commitShuffleTask";
  public static final String FINISH_SHUFFLE_METHOD = "finishShuffle";
  public static final String REQUIRE_BUFFER_METHOD = "requireBuffer";
  public static final String APP_HEARTBEAT_METHOD = "appHeartbeat";
  public static final String REPORT_SHUFFLE_RESULT_METHOD = "reportShuffleResult";
  public static final String GET_SHUFFLE_RESULT_METHOD = "getShuffleResult";
  public static final String GET_SHUFFLE_DATA_METHOD = "getShuffleData";
  public static final String GET_SHUFFLE_INDEX_METHOD = "getShuffleIndex";

  public static synchronized void register(CollectorRegistry collectorRegistry) {
    if (!isRegister) {
      metricsManager = new MetricsManager(collectorRegistry);
      setUpMetrics();
      isRegister = true;
    }
  }

  private static void setUpMetrics() {
    gaugeGrpcOpen = metricsManager.addGauge(GRPC_OPEN);
    counterGrpcTotal = metricsManager.addCounter(GRPC_TOTAL);

    gaugeMap.putIfAbsent(HEARTBEAT_METHOD,
        metricsManager.addGauge(GRPC_HEARTBEAT));
    gaugeMap.putIfAbsent(GET_SHUFFLE_ASSIGNMENTS_METHOD,
        metricsManager.addGauge(GRPC_GET_SHUFFLE_ASSIGNMENTS));

    gaugeMap.putIfAbsent(REGISTER_SHUFFLE_METHOD,
        metricsManager.addGauge(GRPC_REGISTERED_SHUFFLE));
    gaugeMap.putIfAbsent(SEND_SHUFFLE_DATA_METHOD,
        metricsManager.addGauge(GRPC_SEND_SHUFFLE_DATA));
    gaugeMap.putIfAbsent(COMMIT_SHUFFLE_TASK_METHOD,
        metricsManager.addGauge(GRPC_COMMIT_SHUFFLE_TASK));
    gaugeMap.putIfAbsent(FINISH_SHUFFLE_METHOD,
        metricsManager.addGauge(GRPC_FINISH_SHUFFLE));
    gaugeMap.putIfAbsent(REQUIRE_BUFFER_METHOD,
        metricsManager.addGauge(GRPC_REQUIRE_BUFFER));
    gaugeMap.putIfAbsent(APP_HEARTBEAT_METHOD,
        metricsManager.addGauge(GRPC_APP_HEARTBEAT));
    gaugeMap.putIfAbsent(REPORT_SHUFFLE_RESULT_METHOD,
        metricsManager.addGauge(GRPC_REPORT_SHUFFLE_RESULT));
    gaugeMap.putIfAbsent(GET_SHUFFLE_RESULT_METHOD,
        metricsManager.addGauge(GRPC_GET_SHUFFLE_RESULT));
    gaugeMap.putIfAbsent(GET_SHUFFLE_DATA_METHOD,
        metricsManager.addGauge(GRPC_GET_SHUFFLE_DATA));
    gaugeMap.putIfAbsent(GET_SHUFFLE_INDEX_METHOD,
        metricsManager.addGauge(GRPC_GET_SHUFFLE_INDEX));

    counterMap.putIfAbsent(HEARTBEAT_METHOD,
        metricsManager.addCounter(GRPC_HEARTBEAT_TOTAL));
    counterMap.putIfAbsent(GET_SHUFFLE_ASSIGNMENTS_METHOD,
        metricsManager.addCounter(GRPC_GET_SHUFFLE_ASSIGNMENTS_TOTAL));

    counterMap.putIfAbsent(REGISTER_SHUFFLE_METHOD,
        metricsManager.addCounter(GRPC_REGISTERED_SHUFFLE_TOTAL));
    counterMap.putIfAbsent(SEND_SHUFFLE_DATA_METHOD,
        metricsManager.addCounter(GRPC_SEND_SHUFFLE_DATA_TOTAL));
    counterMap.putIfAbsent(COMMIT_SHUFFLE_TASK_METHOD,
        metricsManager.addCounter(GRPC_COMMIT_SHUFFLE_TASK_TOTAL));
    counterMap.putIfAbsent(FINISH_SHUFFLE_METHOD,
        metricsManager.addCounter(GRPC_FINISH_SHUFFLE_TOTAL));
    counterMap.putIfAbsent(REQUIRE_BUFFER_METHOD,
        metricsManager.addCounter(GRPC_REQUIRE_BUFFER_TOTAL));
    counterMap.putIfAbsent(APP_HEARTBEAT_METHOD,
        metricsManager.addCounter(GRPC_APP_HEARTBEAT_TOTAL));
    counterMap.putIfAbsent(REPORT_SHUFFLE_RESULT_METHOD,
        metricsManager.addCounter(GRPC_REPORT_SHUFFLE_RESULT_TOTAL));
    counterMap.putIfAbsent(GET_SHUFFLE_RESULT_METHOD,
        metricsManager.addCounter(GRPC_GET_SHUFFLE_RESULT_TOTAL));
    counterMap.putIfAbsent(GET_SHUFFLE_DATA_METHOD,
        metricsManager.addCounter(GRPC_GET_SHUFFLE_DATA_TOTAL));
    counterMap.putIfAbsent(GET_SHUFFLE_INDEX_METHOD,
        metricsManager.addCounter(GRPC_GET_SHUFFLE_INDEX_TOTAL));
  }

  public static void incCounter(String methodName) {
    if (isRegister) {
      Gauge gauge = gaugeMap.get(methodName);
      if (gauge != null) {
        gauge.inc();
      }
      Counter counter = counterMap.get(methodName);
      if (counter != null) {
        counter.inc();
      }
      gaugeGrpcOpen.inc();
      counterGrpcTotal.inc();
    }
  }

  public static void decCounter(String methodName) {
    if (isRegister) {
      Gauge gauge = gaugeMap.get(methodName);
      if (gauge != null) {
        gauge.dec();
      }
      gaugeGrpcOpen.dec();
    }
  }
}
