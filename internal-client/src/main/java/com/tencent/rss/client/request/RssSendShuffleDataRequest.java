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

package com.tencent.rss.client.request;

import java.util.List;
import java.util.Map;

import com.tencent.rss.common.ShuffleBlockInfo;

public class RssSendShuffleDataRequest {

  private String appId;
  private int retryMax;
  private long retryIntervalMax;
  private Map<Integer, Map<Integer, List<ShuffleBlockInfo>>> shuffleIdToBlocks;

  public RssSendShuffleDataRequest(String appId, int retryMax, long retryIntervalMax,
      Map<Integer, Map<Integer, List<ShuffleBlockInfo>>> shuffleIdToBlocks) {
    this.appId = appId;
    this.retryMax = retryMax;
    this.retryIntervalMax = retryIntervalMax;
    this.shuffleIdToBlocks = shuffleIdToBlocks;
  }

  public String getAppId() {
    return appId;
  }

  public int getRetryMax() {
    return retryMax;
  }

  public long getRetryIntervalMax() {
    return retryIntervalMax;
  }

  public Map<Integer, Map<Integer, List<ShuffleBlockInfo>>> getShuffleIdToBlocks() {
    return shuffleIdToBlocks;
  }
}
