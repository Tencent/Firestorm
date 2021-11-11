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

package com.tencent.rss.client.impl.grpc;

import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;
import com.tencent.rss.client.api.ShuffleServerClient;
import com.tencent.rss.client.request.RssAppHeartBeatRequest;
import com.tencent.rss.client.request.RssFinishShuffleRequest;
import com.tencent.rss.client.request.RssGetShuffleDataRequest;
import com.tencent.rss.client.request.RssGetShuffleIndexRequest;
import com.tencent.rss.client.request.RssGetShuffleResultRequest;
import com.tencent.rss.client.request.RssRegisterShuffleRequest;
import com.tencent.rss.client.request.RssReportShuffleResultRequest;
import com.tencent.rss.client.request.RssSendCommitRequest;
import com.tencent.rss.client.request.RssSendShuffleDataRequest;
import com.tencent.rss.client.response.ResponseStatusCode;
import com.tencent.rss.client.response.RssAppHeartBeatResponse;
import com.tencent.rss.client.response.RssFinishShuffleResponse;
import com.tencent.rss.client.response.RssGetShuffleDataResponse;
import com.tencent.rss.client.response.RssGetShuffleIndexResponse;
import com.tencent.rss.client.response.RssGetShuffleResultResponse;
import com.tencent.rss.client.response.RssRegisterShuffleResponse;
import com.tencent.rss.client.response.RssReportShuffleResultResponse;
import com.tencent.rss.client.response.RssSendCommitResponse;
import com.tencent.rss.client.response.RssSendShuffleDataResponse;
import com.tencent.rss.common.PartitionRange;
import com.tencent.rss.common.ShuffleBlockInfo;
import com.tencent.rss.common.ShuffleIndexResult;
import com.tencent.rss.common.exception.RssException;
import com.tencent.rss.proto.RssProtos.AppHeartBeatRequest;
import com.tencent.rss.proto.RssProtos.AppHeartBeatResponse;
import com.tencent.rss.proto.RssProtos.FinishShuffleRequest;
import com.tencent.rss.proto.RssProtos.FinishShuffleResponse;
import com.tencent.rss.proto.RssProtos.GetShuffleDataRequest;
import com.tencent.rss.proto.RssProtos.GetShuffleDataResponse;
import com.tencent.rss.proto.RssProtos.GetShuffleIndexRequest;
import com.tencent.rss.proto.RssProtos.GetShuffleIndexResponse;
import com.tencent.rss.proto.RssProtos.GetShuffleResultRequest;
import com.tencent.rss.proto.RssProtos.GetShuffleResultResponse;
import com.tencent.rss.proto.RssProtos.PartitionToBlockIds;
import com.tencent.rss.proto.RssProtos.ReportShuffleResultRequest;
import com.tencent.rss.proto.RssProtos.ReportShuffleResultResponse;
import com.tencent.rss.proto.RssProtos.RequireBufferRequest;
import com.tencent.rss.proto.RssProtos.RequireBufferResponse;
import com.tencent.rss.proto.RssProtos.SendShuffleDataRequest;
import com.tencent.rss.proto.RssProtos.SendShuffleDataResponse;
import com.tencent.rss.proto.RssProtos.ShuffleBlock;
import com.tencent.rss.proto.RssProtos.ShuffleCommitRequest;
import com.tencent.rss.proto.RssProtos.ShuffleCommitResponse;
import com.tencent.rss.proto.RssProtos.ShuffleData;
import com.tencent.rss.proto.RssProtos.ShufflePartitionRange;
import com.tencent.rss.proto.RssProtos.ShuffleRegisterRequest;
import com.tencent.rss.proto.RssProtos.ShuffleRegisterResponse;
import com.tencent.rss.proto.RssProtos.StatusCode;
import com.tencent.rss.proto.ShuffleServerGrpc;
import com.tencent.rss.proto.ShuffleServerGrpc.ShuffleServerBlockingStub;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ShuffleServerGrpcClient extends GrpcClient implements ShuffleServerClient {

  private static final Logger LOG = LoggerFactory.getLogger(ShuffleServerGrpcClient.class);
  private static final long FAILED_REQUIRE_ID = -1;
  private static final long RPC_TIMEOUT_DEFAULT_MS = 60000;
  private ShuffleServerBlockingStub blockingStub;

  public ShuffleServerGrpcClient(String host, int port) {
    this(host, port, 3);
  }

  public ShuffleServerGrpcClient(String host, int port, int maxRetryAttempts) {
    this(host, port, maxRetryAttempts, true);
  }

  public ShuffleServerGrpcClient(String host, int port, int maxRetryAttempts, boolean usePlaintext) {
    super(host, port, maxRetryAttempts, usePlaintext);
    blockingStub = ShuffleServerGrpc.newBlockingStub(channel);
  }

  @Override
  public String getDesc() {
    return "Shuffle server grpc client ref " + host + ":" + port;
  }

  private ShuffleRegisterResponse doRegisterShuffle(String appId, int shuffleId, List<PartitionRange> partitionRanges) {
    ShuffleRegisterRequest request = ShuffleRegisterRequest.newBuilder().setAppId(appId)
        .setShuffleId(shuffleId).addAllPartitionRanges(toShufflePartitionRanges(partitionRanges)).build();
    return blockingStub.registerShuffle(request);
  }

  private ShuffleCommitResponse doSendCommit(String appId, int shuffleId) {
    ShuffleCommitRequest request = ShuffleCommitRequest.newBuilder()
        .setAppId(appId).setShuffleId(shuffleId).build();
    int retryNum = 0;
    while (retryNum <= maxRetryAttempts) {
      try {
        ShuffleCommitResponse response = blockingStub.withDeadlineAfter(
            RPC_TIMEOUT_DEFAULT_MS, TimeUnit.MILLISECONDS).commitShuffleTask(request);
        return response;
      } catch (Exception e) {
        retryNum++;
        LOG.warn("Send commit to host[" + host + "], port[" + port
            + "] failed, try again, retryNum[" + retryNum + "]", e);
      }
    }
    throw new RuntimeException("Send commit to host[" + host + "], port[" + port + "] failed");
  }

  private AppHeartBeatResponse doSendHeartBeat(String appId, long timeout) {
    AppHeartBeatRequest request = AppHeartBeatRequest.newBuilder().setAppId(appId).build();
    return blockingStub.withDeadlineAfter(timeout, TimeUnit.MILLISECONDS).appHeartbeat(request);
  }

  public long requirePreAllocation(int requireSize, int retryMax, long retryIntervalMax) {
    RequireBufferRequest rpcRequest = RequireBufferRequest.newBuilder().setRequireSize(requireSize).build();
    RequireBufferResponse rpcResponse = blockingStub.requireBuffer(rpcRequest);
    int retry = 0;
    long result = FAILED_REQUIRE_ID;
    Random random = new Random();
    final int backOffBase = 2000;
    while (rpcResponse.getStatus() == StatusCode.NO_BUFFER) {
      LOG.info("Can't require " + requireSize + " bytes from " + host + ":" + port + ", sleep and try["
          + retry + "] again");
      if (retry >= retryMax) {
        LOG.warn("ShuffleServer " + host + ":" + port + " is full and can't send shuffle"
            + " data successfully after retry " + retryMax + " times");
        return result;
      }
      try {
        long backoffTime =
            Math.min(retryIntervalMax, backOffBase * (1 << Math.min(retry, 16)) + random.nextInt(backOffBase));
        Thread.sleep(backoffTime);
      } catch (Exception e) {
        LOG.warn("Exception happened when require pre allocation", e);
      }
      rpcResponse = blockingStub.requireBuffer(rpcRequest);
      retry++;
    }
    if (rpcResponse.getStatus() == StatusCode.SUCCESS) {
      result = rpcResponse.getRequireBufferId();
    }
    return result;
  }

  @Override
  public RssRegisterShuffleResponse registerShuffle(RssRegisterShuffleRequest request) {
    ShuffleRegisterResponse rpcResponse = doRegisterShuffle(
        request.getAppId(),
        request.getShuffleId(),
        request.getPartitionRanges());

    RssRegisterShuffleResponse response;
    StatusCode statusCode = rpcResponse.getStatus();
    switch (statusCode) {
      case SUCCESS:
        response = new RssRegisterShuffleResponse(ResponseStatusCode.SUCCESS);
        break;
      default:
        String msg = "Can't register shuffle to " + host + ":" + port
            + " for appId[" + request.getAppId() + "], shuffleId[" + request.getShuffleId()
            + "], errorMsg:" + rpcResponse.getRetMsg();
        LOG.error(msg);
        throw new RuntimeException(msg);
    }
    return response;
  }

  @Override
  public RssSendShuffleDataResponse sendShuffleData(RssSendShuffleDataRequest request) {
    String appId = request.getAppId();
    Map<Integer, Map<Integer, List<ShuffleBlockInfo>>> shuffleIdToBlocks = request.getShuffleIdToBlocks();

    List<ShuffleBlockInfo> shuffleBlockInfos = Lists.newArrayList();
    boolean isSuccessful = true;

    // prepare rpc request based on shuffleId -> partitionId -> blocks
    for (Map.Entry<Integer, Map<Integer, List<ShuffleBlockInfo>>> stb : shuffleIdToBlocks.entrySet()) {
      List<ShuffleData> shuffleData = Lists.newArrayList();
      int size = 0;
      int blockNum = 0;
      for (Map.Entry<Integer, List<ShuffleBlockInfo>> ptb : stb.getValue().entrySet()) {
        List<ShuffleBlock> shuffleBlocks = Lists.newArrayList();
        for (ShuffleBlockInfo sbi : ptb.getValue()) {
          shuffleBlockInfos.add(sbi);
          shuffleBlocks.add(ShuffleBlock.newBuilder().setBlockId(sbi.getBlockId())
              .setCrc(sbi.getCrc())
              .setLength(sbi.getLength())
              .setTaskAttemptId(sbi.getTaskAttemptId())
              .setUncompressLength(sbi.getUncompressLength())
              .setData(ByteString.copyFrom(sbi.getData()))
              .build());
          size += sbi.getSize();
          blockNum++;
        }
        shuffleData.add(ShuffleData.newBuilder().setPartitionId(ptb.getKey())
            .addAllBlock(shuffleBlocks)
            .build());
      }

      long requireId = requirePreAllocation(size, request.getRetryMax(), request.getRetryIntervalMax());
      if (requireId != FAILED_REQUIRE_ID) {
        SendShuffleDataRequest rpcRequest = SendShuffleDataRequest.newBuilder()
            .setAppId(appId)
            .setShuffleId(stb.getKey())
            .setRequireBufferId(requireId)
            .addAllShuffleData(shuffleData)
            .build();
        long start = System.currentTimeMillis();
        SendShuffleDataResponse response = doSendData(rpcRequest);
        LOG.info("Do sendShuffleData rpc cost:" + (System.currentTimeMillis() - start)
            + " ms for " + size + " bytes with " + blockNum + " blocks");

        if (response.getStatus() != StatusCode.SUCCESS) {
          String msg = "Can't send shuffle data with " + shuffleBlockInfos.size()
              + " blocks to " + host + ":" + port
              + ", statusCode=" + response.getStatus()
              + ", errorMsg:" + response.getRetMsg();
          LOG.warn(msg);
          isSuccessful = false;
          break;
        }
      } else {
        isSuccessful = false;
        break;
      }
    }

    RssSendShuffleDataResponse response;
    if (isSuccessful) {
      response = new RssSendShuffleDataResponse(ResponseStatusCode.SUCCESS);
    } else {
      response = new RssSendShuffleDataResponse(ResponseStatusCode.INTERNAL_ERROR);
    }
    return response;
  }

  private SendShuffleDataResponse doSendData(SendShuffleDataRequest rpcRequest) {
    int retryNum = 0;
    while (retryNum < maxRetryAttempts) {
      try {
        SendShuffleDataResponse response = blockingStub.withDeadlineAfter(
            RPC_TIMEOUT_DEFAULT_MS, TimeUnit.MILLISECONDS).sendShuffleData(rpcRequest);
        return response;
      } catch (Exception e) {
        retryNum++;
        LOG.warn("Send data to host[" + host + "], port[" + port
            + "] failed, try again, retryNum[" + retryNum + "]", e);
      }
    }
    throw new RuntimeException("Send data to host[" + host + "], port[" + port + "] failed");
  }

  @Override
  public RssSendCommitResponse sendCommit(RssSendCommitRequest request) {
    ShuffleCommitResponse rpcResponse = doSendCommit(request.getAppId(), request.getShuffleId());

    RssSendCommitResponse response;
    if (rpcResponse.getStatus() != StatusCode.SUCCESS) {
      String msg = "Can't commit shuffle data to " + host + ":" + port
          + " for [appId=" + request.getAppId() + ", shuffleId=" + request.getShuffleId() + "], "
          + "errorMsg:" + rpcResponse.getRetMsg();
      LOG.error(msg);
      throw new RuntimeException(msg);
    } else {
      response = new RssSendCommitResponse(ResponseStatusCode.SUCCESS);
      response.setCommitCount(rpcResponse.getCommitCount());
    }
    return response;
  }

  @Override
  public RssAppHeartBeatResponse sendHeartBeat(RssAppHeartBeatRequest request) {
    AppHeartBeatResponse appHeartBeatResponse = doSendHeartBeat(request.getAppId(), request.getTimeoutMs());
    if (appHeartBeatResponse.getStatus() != StatusCode.SUCCESS) {
      String msg = "Can't send heartbeat to " + host + ":" + port
          + " for [appId=" + request.getAppId() + ", timeout=" + request.getTimeoutMs() + "ms], "
          + "errorMsg:" + appHeartBeatResponse.getRetMsg();
      LOG.error(msg);
      return new RssAppHeartBeatResponse(ResponseStatusCode.INTERNAL_ERROR);
    } else {
      return new RssAppHeartBeatResponse(ResponseStatusCode.SUCCESS);
    }
  }

  @Override
  public RssFinishShuffleResponse finishShuffle(RssFinishShuffleRequest request) {
    FinishShuffleRequest rpcRequest = FinishShuffleRequest.newBuilder()
        .setAppId(request.getAppId()).setShuffleId(request.getShuffleId()).build();
    FinishShuffleResponse rpcResponse = blockingStub.finishShuffle(rpcRequest);

    RssFinishShuffleResponse response;
    if (rpcResponse.getStatus() != StatusCode.SUCCESS) {
      String msg = "Can't finish shuffle process to " + host + ":" + port
          + " for [appId=" + request.getAppId() + ", shuffleId=" + request.getShuffleId() + "], "
          + "errorMsg:" + rpcResponse.getRetMsg();
      LOG.error(msg);
      throw new RuntimeException(msg);
    } else {
      response = new RssFinishShuffleResponse(ResponseStatusCode.SUCCESS);
    }
    return response;
  }

  @Override
  public RssReportShuffleResultResponse reportShuffleResult(RssReportShuffleResultRequest request) {
    List<PartitionToBlockIds> partitionToBlockIds = Lists.newArrayList();
    for (Map.Entry<Integer, List<Long>> entry : request.getPartitionToBlockIds().entrySet()) {
      List<Long> blockIds = entry.getValue();
      if (blockIds != null && !blockIds.isEmpty()) {
        partitionToBlockIds.add(PartitionToBlockIds.newBuilder()
            .setPartitionId(entry.getKey())
            .addAllBlockIds(entry.getValue())
            .build());
      }
    }

    ReportShuffleResultRequest recRequest = ReportShuffleResultRequest.newBuilder()
        .setAppId(request.getAppId())
        .setShuffleId(request.getShuffleId())
        .setTaskAttemptId(request.getTaskAttemptId())
        .setBitmapNum(request.getBitmapNum())
        .addAllPartitionToBlockIds(partitionToBlockIds)
        .build();
    ReportShuffleResultResponse rpcResponse = doReportShuffleResult(recRequest);

    StatusCode statusCode = rpcResponse.getStatus();
    RssReportShuffleResultResponse response;
    switch (statusCode) {
      case SUCCESS:
        response = new RssReportShuffleResultResponse(ResponseStatusCode.SUCCESS);
        break;
      default:
        String msg = "Can't report shuffle result to " + host + ":" + port
            + " for [appId=" + request.getAppId() + ", shuffleId=" + request.getShuffleId()
            + ", errorMsg:" + rpcResponse.getRetMsg();
        LOG.error(msg);
        throw new RuntimeException(msg);
    }

    return response;
  }

  private ReportShuffleResultResponse doReportShuffleResult(ReportShuffleResultRequest rpcRequest) {
    int retryNum = 0;
    while (retryNum < maxRetryAttempts) {
      try {
        ReportShuffleResultResponse response = blockingStub.withDeadlineAfter(
            RPC_TIMEOUT_DEFAULT_MS, TimeUnit.MILLISECONDS).reportShuffleResult(rpcRequest);
        return response;
      } catch (Exception e) {
        retryNum++;
        LOG.warn("Report shuffle result to host[" + host + "], port[" + port
            + "] failed, try again, retryNum[" + retryNum + "]", e);
      }
    }
    throw new RuntimeException("Report shuffle result to host[" + host + "], port[" + port + "] failed");
  }

  @Override
  public RssGetShuffleResultResponse getShuffleResult(RssGetShuffleResultRequest request) {
    GetShuffleResultRequest rpcRequest = GetShuffleResultRequest
        .newBuilder()
        .setAppId(request.getAppId())
        .setShuffleId(request.getShuffleId())
        .setPartitionId(request.getPartitionId())
        .build();
    GetShuffleResultResponse rpcResponse = blockingStub.getShuffleResult(rpcRequest);
    StatusCode statusCode = rpcResponse.getStatus();

    RssGetShuffleResultResponse response;
    switch (statusCode) {
      case SUCCESS:
        try {
          response = new RssGetShuffleResultResponse(ResponseStatusCode.SUCCESS,
              rpcResponse.getSerializedBitmap().toByteArray());
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
        break;
      default:
        String msg = "Can't get shuffle result from " + host + ":" + port
            + " for [appId=" + request.getAppId() + ", shuffleId=" + request.getShuffleId()
            + ", errorMsg:" + rpcResponse.getRetMsg();
        LOG.error(msg);
        throw new RssException(msg);
    }

    return response;
  }

  @Override
  public RssGetShuffleDataResponse getShuffleData(RssGetShuffleDataRequest request) {
    GetShuffleDataRequest rpcRequest = GetShuffleDataRequest
        .newBuilder()
        .setAppId(request.getAppId())
        .setShuffleId(request.getShuffleId())
        .setPartitionId(request.getPartitionId())
        .setPartitionNumPerRange(request.getPartitionNumPerRange())
        .setPartitionNum(request.getPartitionNum())
        .setReadBufferSize(request.getReadBufferSize())
        .setOffset(request.getOffset())
        .setLength(request.getLength())
        .build();
    long start = System.currentTimeMillis();
    GetShuffleDataResponse rpcResponse = blockingStub.getShuffleData(rpcRequest);
    String requestInfo = "appId[" + request.getAppId() + "], shuffleId["
        + request.getShuffleId() + "], partitionId[" + request.getPartitionId() + "]";
    LOG.info("GetShuffleData for " + requestInfo + " cost " + (System.currentTimeMillis() - start) + " ms");

    StatusCode statusCode = rpcResponse.getStatus();

    RssGetShuffleDataResponse response;
    switch (statusCode) {
      case SUCCESS:
        response = new RssGetShuffleDataResponse(ResponseStatusCode.SUCCESS);
        response.setShuffleData(rpcResponse.getData().toByteArray());
        break;
      default:
        String msg = "Can't get shuffle data from " + host + ":" + port
            + " for " + requestInfo + ", errorMsg:" + rpcResponse.getRetMsg();
        LOG.error(msg);
        throw new RssException(msg);
    }
    return response;
  }

  @Override
  public RssGetShuffleIndexResponse getShuffleIndex(RssGetShuffleIndexRequest request) {
    GetShuffleIndexRequest rpcRequest = GetShuffleIndexRequest
        .newBuilder()
        .setAppId(request.getAppId())
        .setShuffleId(request.getShuffleId())
        .setPartitionId(request.getPartitionId())
        .setPartitionNumPerRange(request.getPartitionNumPerRange())
        .setPartitionNum(request.getPartitionNum())
        .build();
    long start = System.currentTimeMillis();
    GetShuffleIndexResponse rpcResponse = blockingStub.getShuffleIndex(rpcRequest);
    String requestInfo = "appId[" + request.getAppId() + "], shuffleId["
        + request.getShuffleId() + "], partitionId[" + request.getPartitionId() + "]";
    LOG.info("GetShuffleIndex for " + requestInfo + " cost " + (System.currentTimeMillis() - start) + " ms");

    StatusCode statusCode = rpcResponse.getStatus();

    RssGetShuffleIndexResponse response;
    switch (statusCode) {
      case SUCCESS:
        response = new RssGetShuffleIndexResponse(ResponseStatusCode.SUCCESS);
        if (!rpcResponse.getIndexData().isEmpty()) {
          response.setShuffleIndexResult(
              new ShuffleIndexResult(rpcResponse.getIndexData().toByteArray()));
        }
        break;
      default:
        String msg = "Can't get shuffle index from " + host + ":" + port
            + " for " + requestInfo + ", errorMsg:" + rpcResponse.getRetMsg();
        LOG.error(msg);
        throw new RssException(msg);
    }
    return response;
  }


  @Override
  public String getClientInfo() {
    return "ShuffleServerGrpcClient for host[" + host + "], port[" + port + "]";
  }


  private List<ShufflePartitionRange> toShufflePartitionRanges(List<PartitionRange> partitionRanges) {
    List<ShufflePartitionRange> ret = Lists.newArrayList();
    for (PartitionRange partitionRange : partitionRanges) {
      ret.add(ShufflePartitionRange
          .newBuilder()
          .setStart(partitionRange.getStart())
          .setEnd(partitionRange.getEnd()).build());
    }
    return ret;
  }
}
