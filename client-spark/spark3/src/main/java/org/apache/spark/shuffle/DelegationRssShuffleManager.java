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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.ShuffleDependency;
import org.apache.spark.SparkConf;
import org.apache.spark.TaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tencent.rss.client.api.CoordinatorClient;
import com.tencent.rss.client.request.RssAccessClusterRequest;
import com.tencent.rss.client.response.ResponseStatusCode;
import com.tencent.rss.client.response.RssAccessClusterResponse;
import com.tencent.rss.common.exception.RssException;
import com.tencent.rss.common.util.Constants;

public class DelegationRssShuffleManager implements ShuffleManager {

  private static final Logger LOG = LoggerFactory.getLogger(DelegationRssShuffleManager.class);

  private final ShuffleManager delegate;
  private final String sortShuffleManagerName;
  private final List<CoordinatorClient> coordinatorClients;
  private final int accessTimeoutMs;
  private final SparkConf sparkConf;
  private String accessId;

  public DelegationRssShuffleManager(SparkConf sparkConf, boolean isDriver) throws Exception {
    this.sparkConf = sparkConf;
    sortShuffleManagerName = sparkConf.get(
        RssClientConfig.RSS_SORT_SHUFFLE_MANAGER_IMPL,
        RssClientConfig.RSS_SORT_SHUFFLE_MANAGER_IMPL_DEFAULT_VALUE);
    accessTimeoutMs = sparkConf.getInt(
        RssClientConfig.RSS_ACCESS_TIMEOUT_MS,
        RssClientConfig.RSS_ACCESS_TIMEOUT_MS_DEFAULT_VALUE);
    if (isDriver) {
      coordinatorClients = RssShuffleUtils.createCoordinatorClients(sparkConf);
      delegate = createShuffleManagerInDriver();
    } else {
      coordinatorClients = Lists.newArrayList();
      delegate = createShuffleManagerInExecutor();
    }

    if (delegate == null) {
      throw new RssException("Fail to create shuffle manager!");
    }
  }

  private ShuffleManager createShuffleManagerInDriver() throws RssException {
    ShuffleManager shuffleManager;

    boolean canAccess = tryAccessCluster();
    if (canAccess) {
      try {
        shuffleManager = new RssShuffleManager(sparkConf, true);
        sparkConf.set(RssClientConfig.RSS_USE_RSS_SHUFFLE_MANAGER, "true");
        return shuffleManager;
      } catch (Exception exception) {
        LOG.warn("Fail to create RssShuffleManager, fallback to SortShuffleManager");
      }
    }

    try {
      shuffleManager = RssShuffleUtils.loadShuffleManager(sortShuffleManagerName, sparkConf, true);
      sparkConf.set(RssClientConfig.RSS_USE_RSS_SHUFFLE_MANAGER, "false");
    } catch (Exception e) {
      throw new RssException(e.getMessage());
    }

    return shuffleManager;
  }

  private boolean tryAccessCluster() {
    String accessIdStr = getAndCheckAccessIdStr();
    if (StringUtils.isEmpty(accessIdStr)) {
      return false;
    }

    if (!extractAccessId(accessIdStr)) {
      return false;
    }

    for (CoordinatorClient coordinatorClient : coordinatorClients) {
      try {
        RssAccessClusterResponse response =
            coordinatorClient.accessCluster(new RssAccessClusterRequest(
                accessId, Sets.newHashSet(Constants.SHUFFLE_SERVER_VERSION), accessTimeoutMs));
        if (response.getStatusCode() == ResponseStatusCode.SUCCESS) {
          LOG.warn("Success to access cluster {} using {}", coordinatorClient.getDesc(), accessId);
          return true;
        }
        LOG.warn("Fail to access cluster {} using {} for {}",
            coordinatorClient.getDesc(), accessId, response.getMessage());
      } catch (Exception e) {
        LOG.warn("Fail to access cluster {} using {} for {}",
            coordinatorClient.getDesc(), accessId, e.getMessage());
      }
    }

    return false;
  }

  private String getAndCheckAccessIdStr() {
    String accessInfoKey = sparkConf.get(
        RssClientConfig.RSS_ACCESS_ID_KEY,
        RssClientConfig.RSS_ACCESS_ID_KEY_DEFAULT_VALUE);
    if (StringUtils.isEmpty(accessInfoKey)) {
      LOG.warn("Access id key is empty");
      return null;
    }

    String accessId = sparkConf.get(accessInfoKey, RssClientConfig.RSS_ACCESS_ID_DEFAULT_VALUE);
    if (StringUtils.isEmpty(accessId)) {
      LOG.warn("Access id is empty");
      return null;
    }

    return accessId;
  }

  private boolean extractAccessId(String accessIdStr) {
    String patternStr = sparkConf.get(RssClientConfig.RSS_ACCESS_ID_PATTERN,
        RssClientConfig.RSS_ACCESS_ID_PATTERN_DEFAULT_VALUE);
    if (StringUtils.isEmpty(patternStr)) {
      LOG.warn("Access pattern is empty");
      return false;
    }

    try {
      Pattern pattern = Pattern.compile(patternStr);
      Matcher matcher = pattern.matcher(accessIdStr);
      if (matcher.find()) {
        accessId = matcher.group(1);
      } else {
        LOG.warn("No match found for access id str {} in pattern {} may be wrong", accessIdStr, patternStr);
        return false;
      }

      if (StringUtils.isEmpty(accessId)) {
        LOG.warn("Access id is empty");
        return false;
      }
    } catch (Exception e) {
      LOG.warn("Fail to extract access id {} using pattern {} for {}", accessIdStr, patternStr, e.getMessage());
      return false;
    }

    return true;
  }

  private ShuffleManager createShuffleManagerInExecutor() throws RssException {
    ShuffleManager shuffleManager;
    // get useRSS from spark conf
    boolean useRSS = Boolean.parseBoolean(sparkConf.get(
        RssClientConfig.RSS_USE_RSS_SHUFFLE_MANAGER,
        RssClientConfig.RSS_USE_RSS_SHUFFLE_MANAGER_DEFAULT_VALUE));
    if (useRSS) {
      // Executor will not do any fallback
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

  public ShuffleManager getDelegate() {
    return delegate;
  }

  public String getAccessId() {
    return accessId;
  }

  @Override
  public <K, V, C> ShuffleHandle registerShuffle(int shuffleId, int numMaps, ShuffleDependency<K, V, C> dependency) {
    return delegate.registerShuffle(shuffleId, numMaps, dependency);
  }

  @Override
  public <K, V> ShuffleWriter<K, V> getWriter(ShuffleHandle handle, int mapId, TaskContext context) {
    return delegate.getWriter(handle, mapId, context);
  }

  @Override
  public <K, C> ShuffleReader<K, C> getReader(
      ShuffleHandle handle, int startPartition, int endPartition, TaskContext context) {
    return delegate.getReader(handle, startPartition, endPartition, context);
  }

  @Override
  public boolean unregisterShuffle(int shuffleId) {
    return delegate.unregisterShuffle(shuffleId);
  }

  @Override
  public void stop() {
    delegate.stop();
  }

  @Override
  public ShuffleBlockResolver shuffleBlockResolver() {
    return delegate.shuffleBlockResolver();
  }
}
