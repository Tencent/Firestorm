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

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.ShuffleDependency;
import org.apache.spark.SparkConf;
import org.apache.spark.TaskContext;

import com.tencent.rss.client.api.CoordinatorClient;
import com.tencent.rss.common.exception.RssException;

import static org.apache.spark.shuffle.RssClientConfig.RSS_ACCESS_INFO_DEFAULT_VALUE;
import static org.apache.spark.shuffle.RssClientConfig.RSS_ACCESS_INFO_KEY;
import static org.apache.spark.shuffle.RssClientConfig.RSS_ACCESS_INFO_KEY_DEFAULT_VALUE;
import static org.apache.spark.shuffle.RssClientConfig.RSS_SORT_SHUFFLE_MANAGER_IMPL;
import static org.apache.spark.shuffle.RssClientConfig.RSS_SORT_SHUFFLE_MANAGER_IMPL_DEFAULT_VALUE;
import static org.apache.spark.shuffle.RssClientConfig.RSS_USE_RSS_SHUFFLE_MANAGER;
import static org.apache.spark.shuffle.RssClientConfig.RSS_USE_RSS_SHUFFLE_MANAGER_DEFAULT_VALUE;

public class DelegationRssShuffleManager implements ShuffleManager {

  private final ShuffleManager delegate;
  private final String sortShuffleManagerName;
  private final List<CoordinatorClient> coordinatorClients;

  public DelegationRssShuffleManager(SparkConf sparkConf, boolean isDriver) throws Exception {
    sortShuffleManagerName = sparkConf.get(RSS_SORT_SHUFFLE_MANAGER_IMPL, RSS_SORT_SHUFFLE_MANAGER_IMPL_DEFAULT_VALUE);
    if (isDriver) {
      delegate = createShuffleManagerInDriver(sparkConf);
      coordinatorClients = RssShuffleUtils.createCoordinatorClients(sparkConf);
    } else {
      delegate = createShuffleManagerInExecutor(sparkConf);
      coordinatorClients = null;
    }

    if (delegate == null) {
      throw new RssException("Fail to create shuffle manager!");
    }
  }

  private ShuffleManager createShuffleManagerInDriver(SparkConf sparkConf) throws RssException {
    ShuffleManager shuffleManager;
    String accessInfoKey = sparkConf.get(RSS_ACCESS_INFO_KEY, RSS_ACCESS_INFO_KEY_DEFAULT_VALUE);
    if (StringUtils.isEmpty(accessInfoKey)) {
      return null;
    }
    String accessInfo = sparkConf.get(accessInfoKey, RSS_ACCESS_INFO_DEFAULT_VALUE);
    if (StringUtils.isEmpty(accessInfo)) {
      return null;
    }

    // TODO: send access request
    if (true) {
      shuffleManager = new RssShuffleManager(sparkConf, true);
      sparkConf.set(RSS_USE_RSS_SHUFFLE_MANAGER, "true");
    } else {
      try {
        shuffleManager = RssShuffleUtils.loadShuffleManager(sortShuffleManagerName, sparkConf, false);
      } catch (Exception e) {
        throw new RssException(e.getMessage());
      }
    }

    return shuffleManager;
  }

  private ShuffleManager createShuffleManagerInExecutor(SparkConf sparkConf) throws RssException {
    ShuffleManager shuffleManager;
    // get useRSS from spark conf
    boolean useRSS = sparkConf.getBoolean(RSS_USE_RSS_SHUFFLE_MANAGER, RSS_USE_RSS_SHUFFLE_MANAGER_DEFAULT_VALUE);
    if (useRSS) {
      shuffleManager = new RssShuffleManager(sparkConf, false);
    } else {
      try {
        shuffleManager = RssShuffleUtils.loadShuffleManager(sortShuffleManagerName, sparkConf, false);
      } catch (Exception e) {
        throw new RssException(e.getMessage());
      }
    }
    return shuffleManager;
  }

  @Override
  public <K, V, C> ShuffleHandle registerShuffle(int shuffleId, ShuffleDependency<K, V, C> dependency) {
    return delegate.registerShuffle(shuffleId, dependency);
  }

  @Override
  public <K, V> ShuffleWriter<K, V> getWriter(
      ShuffleHandle handle,
      long mapId,
      TaskContext context,
      ShuffleWriteMetricsReporter metrics) {
    return delegate.getWriter(handle, mapId, context, metrics);
  }

  @Override
  public <K, C> ShuffleReader<K, C> getReader(
      ShuffleHandle handle,
      int startPartition,
      int endPartition,
      TaskContext context,
      ShuffleReadMetricsReporter metrics) {
    return delegate.getReader(handle, startPartition, endPartition, context, metrics);
  }

  public <K, C> ShuffleReader<K, C> getReader(
      ShuffleHandle handle,
      int startMapIndex,
      int endMapIndex,
      int startPartition,
      int endPartition,
      TaskContext context,
      ShuffleReadMetricsReporter metrics) {
    return delegate.getReader(handle, startMapIndex, endMapIndex, startPartition, endPartition, context, metrics);
  }

  @Override
  public boolean unregisterShuffle(int shuffleId) {
    return delegate.unregisterShuffle(shuffleId);
  }

  @Override
  public ShuffleBlockResolver shuffleBlockResolver() {
    return delegate.shuffleBlockResolver();
  }

  @Override
  public void stop() {
    delegate.stop();
  }
}
