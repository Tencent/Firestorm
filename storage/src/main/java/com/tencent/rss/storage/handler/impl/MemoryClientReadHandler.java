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

package com.tencent.rss.storage.handler.impl;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tencent.rss.client.api.ShuffleServerClient;
import com.tencent.rss.client.request.RssGetInMemoryShuffleDataRequest;
import com.tencent.rss.client.response.RssGetInMemoryShuffleDataResponse;
import com.tencent.rss.common.BufferSegment;
import com.tencent.rss.common.ShuffleDataResult;
import com.tencent.rss.common.exception.RssException;
import com.tencent.rss.common.util.Constants;

public class MemoryClientReadHandler extends AbstractClientReadHandler {

  private static final Logger LOG = LoggerFactory.getLogger(MemoryQuorumClientReadHandler.class);
  private long lastBlockId = Constants.INVALID_BLOCK_ID;
  private ShuffleServerClient shuffleServerClient;

  public MemoryClientReadHandler(
      String appId,
      int shuffleId,
      int partitionId,
      int readBufferSize,
      ShuffleServerClient shuffleServerClient) {
    this.appId = appId;
    this.shuffleId = shuffleId;
    this.partitionId = partitionId;
    this.readBufferSize = readBufferSize;
    this.shuffleServerClient = shuffleServerClient;
  }

  @Override
  public ShuffleDataResult readShuffleData() {
    ShuffleDataResult result = null;

    RssGetInMemoryShuffleDataRequest request = new RssGetInMemoryShuffleDataRequest(
      appId,shuffleId, partitionId, lastBlockId, readBufferSize);

    try {
      RssGetInMemoryShuffleDataResponse response =
          shuffleServerClient.getInMemoryShuffleData(request);
      result = new ShuffleDataResult(response.getData(), response.getBufferSegments());
    } catch (Exception e) {
      // todo: fault tolerance solution should be added
      throw new RssException("Failed to read in memory shuffle data with "
          + shuffleServerClient.getClientInfo() + " due to " + e);
    }

    // update lastBlockId for next rpc call
    if (!result.isEmpty()) {
      List<BufferSegment> bufferSegments = result.getBufferSegments();
      lastBlockId = bufferSegments.get(bufferSegments.size() - 1).getBlockId();
    }

    return result;
  }
}
