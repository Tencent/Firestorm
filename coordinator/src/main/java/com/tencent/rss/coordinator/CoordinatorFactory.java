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

import com.tencent.rss.common.rpc.GrpcServer;
import com.tencent.rss.common.rpc.ServerInterface;

public class CoordinatorFactory {

  private final CoordinatorServer coordinatorServer;
  private final CoordinatorConf conf;

  public CoordinatorFactory(CoordinatorServer coordinatorServer) {
    this.coordinatorServer = coordinatorServer;
    this.conf = coordinatorServer.getCoordinatorConf();
  }

  public ServerInterface getServer() {
    String type = conf.getString(CoordinatorConf.RPC_SERVER_TYPE);
    if (type.equals(ServerType.GRPC.name())) {
      return new GrpcServer(conf, new CoordinatorGrpcService(coordinatorServer),
          coordinatorServer.getGrpcMetrics());
    } else {
      throw new UnsupportedOperationException("Unsupported server type " + type);
    }
  }

  enum ServerType {
    GRPC
  }
}
