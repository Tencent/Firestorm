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

package com.tencent.rss.test;

import com.tencent.rss.client.impl.grpc.CoordinatorGrpcClient;
import com.tencent.rss.proto.RssProtos;

public class CoordinatorTestUtils {

  private CoordinatorTestUtils() {
  }

  public static void waitForRegister(CoordinatorGrpcClient coordinatorClient, int expectedServers) throws Exception {
    RssProtos.GetShuffleServerListResponse response;
    int count = 0;
    do {
      response = coordinatorClient.getShuffleServerList();
      Thread.sleep(1000);
      if (count > 10) {
        throw new RuntimeException("No shuffle server connected");
      }
      count++;
    } while (response.getServersCount() < expectedServers);
  }
}
