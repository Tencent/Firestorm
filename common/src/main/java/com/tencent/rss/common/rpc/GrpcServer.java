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

package com.tencent.rss.common.rpc;

import com.google.common.collect.Queues;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.tencent.rss.common.config.RssBaseConf;
import com.tencent.rss.common.util.ExitUtils;
import io.grpc.BindableService;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.ServerInterceptors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class GrpcServer implements ServerInterface {

  private static final Logger LOG = LoggerFactory.getLogger(GrpcServer.class);

  private final Server server;
  private final int port;

  public GrpcServer(RssBaseConf conf, BindableService service) {
    this.port = conf.getInteger(RssBaseConf.RPC_SERVER_PORT);
    int maxInboundMessageSize = conf.getInteger(RssBaseConf.RPC_MESSAGE_MAX_SIZE);
    int rpcExecutorSize = conf.getInteger(RssBaseConf.RPC_EXECUTOR_SIZE);
    ExecutorService pool = new ThreadPoolExecutor(
        rpcExecutorSize,
        rpcExecutorSize * 2,
        10,
        TimeUnit.MINUTES,
        Queues.newLinkedBlockingQueue(Integer.MAX_VALUE),
        new ThreadFactoryBuilder().setDaemon(true).setNameFormat("Grpc-%d").build()
    );

    boolean isMetricsEnabled = conf.getBoolean(RssBaseConf.RPC_METRICS_ENABLED);
    if (isMetricsEnabled) {
      MonitoringServerInterceptor monitoringInterceptor = new MonitoringServerInterceptor();
      this.server = ServerBuilder
          .forPort(port)
          .addService(ServerInterceptors.intercept(service, monitoringInterceptor))
          .executor(pool)
          .maxInboundMessageSize(maxInboundMessageSize)
          .build();
    } else {
      this.server = ServerBuilder
          .forPort(port)
          .addService(service)
          .executor(pool)
          .maxInboundMessageSize(maxInboundMessageSize)
          .build();
    }
  }

  public void start() throws IOException {
    try {
      server.start();
    } catch (IOException e) {
      ExitUtils.terminate(1, "Fail to start grpc server", e, LOG);
    }
    LOG.info("Grpc server started, listening on {}.", port);
  }

  public void stop() throws InterruptedException {
    if (server != null) {
      server.shutdown().awaitTermination(10, TimeUnit.SECONDS);
      LOG.info("GRPC server stopped!");
    }
  }

  public void blockUntilShutdown() throws InterruptedException {
    if (server != null) {
      server.awaitTermination();
    }
  }

}
