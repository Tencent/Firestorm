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

package com.tencent.rss.storage.common;

import java.util.Map;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;

import com.tencent.rss.common.util.RssUtils;
import com.tencent.rss.storage.handler.api.ShuffleWriteHandler;
import com.tencent.rss.storage.request.CreateShuffleWriteHandlerRequest;

public abstract class AbstractStorage implements Storage {

  private Map<String, Map<String, ShuffleWriteHandler>> handlers = Maps.newConcurrentMap();

  abstract ShuffleWriteHandler newWriteHandler(CreateShuffleWriteHandlerRequest request);

  @Override
  public ShuffleWriteHandler createWriteHandler(CreateShuffleWriteHandlerRequest request) {

    handlers.putIfAbsent(request.getAppId(), Maps.newConcurrentMap());
    Map<String, ShuffleWriteHandler> map = handlers.get(request.getAppId());
    map.computeIfAbsent(RssUtils.generatePartitionKey(
        request.getAppId(),
        request.getShuffleId(),
        request.getStartPartition()
    ), key -> newWriteHandler(request));
    return map.get(RssUtils.generatePartitionKey(
        request.getAppId(),
        request.getShuffleId(),
        request.getStartPartition()
    ));
  }

  @Override
  public void removeHandlers(String appId) {
    handlers.remove(appId);
  }

  @VisibleForTesting
  public int getHandlerSize() {
    return handlers.size();
  }
}
