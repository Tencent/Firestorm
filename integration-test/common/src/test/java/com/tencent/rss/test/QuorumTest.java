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

package com.tencent.rss.test;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.io.Files;
import com.tencent.rss.client.factory.ShuffleServerClientFactory;
import com.tencent.rss.client.impl.ShuffleReadClientImpl;
import com.tencent.rss.client.impl.ShuffleWriteClientImpl;
import com.tencent.rss.client.impl.grpc.GrpcClient;
import com.tencent.rss.client.impl.grpc.ShuffleServerGrpcClient;
import com.tencent.rss.client.response.CompressedShuffleBlock;
import com.tencent.rss.client.response.SendShuffleDataResult;
import com.tencent.rss.client.util.ClientType;
import com.tencent.rss.common.PartitionRange;
import com.tencent.rss.common.ShuffleBlockInfo;
import com.tencent.rss.common.ShuffleServerInfo;
import com.tencent.rss.coordinator.CoordinatorConf;
import com.tencent.rss.server.ShuffleServerConf;
import com.tencent.rss.storage.util.StorageType;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

import java.io.File;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

public class QuorumTest extends ShuffleReadWriteBase {

  private static final String EXPECTED_EXCEPTION_MESSAGE = "Exception should be thrown";
  private static ShuffleServerInfo shuffleServerInfo1;
  private static ShuffleServerInfo shuffleServerInfo2;
  private static ShuffleServerInfo shuffleServerInfo3;
  private ShuffleWriteClientImpl shuffleWriteClientImpl;

  @BeforeClass
  public static void setupServers() throws Exception {
    CoordinatorConf coordinatorConf = getCoordinatorConf();
    createCoordinatorServer(coordinatorConf);

    ShuffleServerConf shuffleServerConf = getShuffleServerConf();
    shuffleServerConf.setLong("rss.server.app.expired.withoutHeartbeat", 4000);
    shuffleServerConf.setLong("rss.server.app.expired.withoutHeartbeat", 4000);
    File tmpDir = Files.createTempDir();
    tmpDir.deleteOnExit();

    File dataDir1 = new File(tmpDir, "data1");
    File dataDir2 = new File(tmpDir, "data2");
    String basePath = dataDir1.getAbsolutePath() + "," + dataDir2.getAbsolutePath();
    shuffleServerConf.setString("rss.storage.type", StorageType.MEMORY_LOCALFILE.name());
    shuffleServerConf.setString("rss.storage.basePath", basePath);
    createMockedShuffleServer(shuffleServerConf);

    File dataDir3 = new File(tmpDir, "data3");
    File dataDir4 = new File(tmpDir, "data4");
    basePath = dataDir3.getAbsolutePath() + "," + dataDir4.getAbsolutePath();
    shuffleServerConf.setString("rss.storage.basePath", basePath);
    shuffleServerConf.setInteger("rss.rpc.server.port", SHUFFLE_SERVER_PORT + 1);
    shuffleServerConf.setInteger("rss.jetty.http.port", 18081);
    createMockedShuffleServer(shuffleServerConf);

    File dataDir5 = new File(tmpDir, "data5");
    File dataDir6 = new File(tmpDir, "data6");
    basePath = dataDir5.getAbsolutePath() + "," + dataDir6.getAbsolutePath();
    shuffleServerConf.setString("rss.storage.basePath", basePath);
    shuffleServerConf.setInteger("rss.rpc.server.port", SHUFFLE_SERVER_PORT + 2);
    shuffleServerConf.setInteger("rss.jetty.http.port", 17081);
    createMockedShuffleServer(shuffleServerConf);

    startServers();
    shuffleServerInfo1 =
      new ShuffleServerInfo("127.0.0.1-20001", shuffleServers.get(0).getIp(), SHUFFLE_SERVER_PORT);
    shuffleServerInfo2 =
      new ShuffleServerInfo("127.0.0.1-20002", shuffleServers.get(1).getIp(), SHUFFLE_SERVER_PORT + 1);
    shuffleServerInfo3 =
      new ShuffleServerInfo("127.0.0.1-20003", shuffleServers.get(1).getIp(), SHUFFLE_SERVER_PORT + 2);
  }

  @Before
  public void createClient() {
    // spark.rss.data.replica=3
    // spark.rss.data.replica.write=2
    // spark.rss.data.replica.read=2
    shuffleWriteClientImpl = new ShuffleWriteClientImpl(ClientType.GRPC.name(), 3, 1000, 1,
      3, 2, 2);
    ((ShuffleServerGrpcClient)ShuffleServerClientFactory
      .getInstance().getShuffleServerClient("GRPC", shuffleServerInfo1)).adjustTimeout(10);
    ((ShuffleServerGrpcClient)ShuffleServerClientFactory
      .getInstance().getShuffleServerClient("GRPC", shuffleServerInfo2)).adjustTimeout(10);
    ((ShuffleServerGrpcClient)ShuffleServerClientFactory
      .getInstance().getShuffleServerClient("GRPC", shuffleServerInfo3)).adjustTimeout(10);
  }

  @After
  public void closeClient() {
    shuffleWriteClientImpl.close();
  }

  @Test
  public void rpcFailedTest() throws Exception {
    String testAppId = "rpcFailedTest";
    shuffleWriteClientImpl.registerShuffle(shuffleServerInfo1,
      testAppId, 0, Lists.newArrayList(new PartitionRange(0, 0)));
    shuffleWriteClientImpl.registerShuffle(shuffleServerInfo2,
      testAppId, 0, Lists.newArrayList(new PartitionRange(0, 0)));
    shuffleWriteClientImpl.registerShuffle(shuffleServerInfo3,
      testAppId, 0, Lists.newArrayList(new PartitionRange(0, 0)));
    Map<Long, byte[]> expectedData = Maps.newHashMap();
    Roaring64NavigableMap blockIdBitmap = Roaring64NavigableMap.bitmapOf();

    // simulator of failed servers
    ShuffleServerInfo fakeShuffleServerInfo1 =
      new ShuffleServerInfo("127.0.0.1-20001", shuffleServers.get(0).getIp(), SHUFFLE_SERVER_PORT + 100);
    ShuffleServerInfo fakeShuffleServerInfo2 =
      new ShuffleServerInfo("127.0.0.1-20002", shuffleServers.get(1).getIp(), SHUFFLE_SERVER_PORT + 100);
    ShuffleServerInfo fakeShuffleServerInfo3 =
      new ShuffleServerInfo("127.0.0.1-20003", shuffleServers.get(2).getIp(), SHUFFLE_SERVER_PORT + 100);

    // case1: When only 1 server is failed, the block sending should success
    List<ShuffleBlockInfo> blocks = createShuffleBlockList(
      0, 0, 0, 3, 25, blockIdBitmap,
      expectedData, Lists.newArrayList(shuffleServerInfo1, shuffleServerInfo2, fakeShuffleServerInfo3));

    Roaring64NavigableMap taskIdBitmap = Roaring64NavigableMap.bitmapOf(0);
    SendShuffleDataResult result = shuffleWriteClientImpl.sendShuffleData(testAppId, blocks);
    Roaring64NavigableMap failedBlockIdBitmap = Roaring64NavigableMap.bitmapOf();
    Roaring64NavigableMap succBlockIdBitmap = Roaring64NavigableMap.bitmapOf();
    for (Long blockId : result.getSuccessBlockIds()) {
      succBlockIdBitmap.addLong(blockId);
    }
    for (Long blockId : result.getFailedBlockIds()) {
      failedBlockIdBitmap.addLong(blockId);
    }
    assertEquals(0, failedBlockIdBitmap.getLongCardinality());
    assertEquals(blockIdBitmap, succBlockIdBitmap);

    ShuffleReadClientImpl readClient = new ShuffleReadClientImpl(StorageType.MEMORY_LOCALFILE.name(),
      testAppId, 0, 0, 100, 1,
      10, 1000, "", blockIdBitmap, taskIdBitmap,
      Lists.newArrayList(shuffleServerInfo1, shuffleServerInfo2, fakeShuffleServerInfo3), null);
    // The data should be read
    validateResult(readClient, expectedData);

    // case2: When 2 servers are failed, the block sending should fail
    blockIdBitmap = Roaring64NavigableMap.bitmapOf();
    blocks = createShuffleBlockList(
      0, 0, 0, 3, 25, blockIdBitmap,
      expectedData, Lists.newArrayList(shuffleServerInfo1, fakeShuffleServerInfo2, fakeShuffleServerInfo3));
    result = shuffleWriteClientImpl.sendShuffleData(testAppId, blocks);
    failedBlockIdBitmap = Roaring64NavigableMap.bitmapOf();
    succBlockIdBitmap = Roaring64NavigableMap.bitmapOf();
    for (Long blockId : result.getSuccessBlockIds()) {
      succBlockIdBitmap.addLong(blockId);
    }
    for (Long blockId : result.getFailedBlockIds()) {
      failedBlockIdBitmap.addLong(blockId);
    }
    assertEquals(blockIdBitmap, failedBlockIdBitmap);
    assertEquals(0, succBlockIdBitmap.getLongCardinality());

    // The client should not read any data, because write is failed
    assertEquals(readClient.readShuffleBlockData(), null);
  }

  private void enableTimeout(MockedShuffleServer server, long timeout) {
    ((MockedGrpcServer)server.getServer()).getService()
      .enableMockedTimeout(timeout);
  }

  @Test
  public void rpcTimeoutTest() throws Exception {
    String testAppId = "rpcTimeoutTest";
    shuffleWriteClientImpl.registerShuffle(shuffleServerInfo1,
      testAppId, 0, Lists.newArrayList(new PartitionRange(0, 0)));
    shuffleWriteClientImpl.registerShuffle(shuffleServerInfo2,
      testAppId, 0, Lists.newArrayList(new PartitionRange(0, 0)));
    shuffleWriteClientImpl.registerShuffle(shuffleServerInfo3,
      testAppId, 0, Lists.newArrayList(new PartitionRange(0, 0)));
    Map<Long, byte[]> expectedData = Maps.newHashMap();
    Roaring64NavigableMap blockIdBitmap = Roaring64NavigableMap.bitmapOf();

    // case1: only 1 server is timout, the block sending should success
    enableTimeout((MockedShuffleServer)shuffleServers.get(2), 1000);

    List<ShuffleBlockInfo> blocks = createShuffleBlockList(
      0, 0, 0, 3, 25, blockIdBitmap,
      expectedData, Lists.newArrayList(shuffleServerInfo1, shuffleServerInfo2, shuffleServerInfo3));

    // report result should success
    Map<Integer, List<Long>> partitionToBlockIds = Maps.newHashMap();
    partitionToBlockIds.put(0, Lists.newArrayList(blockIdBitmap.stream().iterator()));
    Map<Integer, List<ShuffleServerInfo>> partitionToServers = Maps.newHashMap();
    partitionToServers.put(0, Lists.newArrayList(shuffleServerInfo1, shuffleServerInfo2, shuffleServerInfo3));
    shuffleWriteClientImpl.reportShuffleResult(partitionToServers, testAppId, 0, 0L,
      partitionToBlockIds, 1);
    Roaring64NavigableMap report = shuffleWriteClientImpl.getShuffleResult("GRPC",
      Sets.newHashSet(shuffleServerInfo1, shuffleServerInfo2, shuffleServerInfo3),
      testAppId, 0, 0);
    assertEquals(report, blockIdBitmap);

    // data read should success
    Roaring64NavigableMap taskIdBitmap = Roaring64NavigableMap.bitmapOf(0);
    SendShuffleDataResult result = shuffleWriteClientImpl.sendShuffleData(testAppId, blocks);
    Roaring64NavigableMap failedBlockIdBitmap = Roaring64NavigableMap.bitmapOf();
    Roaring64NavigableMap succBlockIdBitmap = Roaring64NavigableMap.bitmapOf();
    for (Long blockId : result.getSuccessBlockIds()) {
      succBlockIdBitmap.addLong(blockId);
    }
    for (Long blockId : result.getFailedBlockIds()) {
      failedBlockIdBitmap.addLong(blockId);
    }
    assertEquals(0, failedBlockIdBitmap.getLongCardinality());
    assertEquals(blockIdBitmap, succBlockIdBitmap);

    ShuffleReadClientImpl readClient = new ShuffleReadClientImpl(StorageType.MEMORY_LOCALFILE.name(),
      testAppId, 0, 0, 100, 1,
      10, 1000, "", blockIdBitmap, taskIdBitmap,
      Lists.newArrayList(shuffleServerInfo1, shuffleServerInfo2, shuffleServerInfo3), null);
    validateResult(readClient, expectedData);

    //case2: 2 servers are failed, the block sending should fail
    enableTimeout((MockedShuffleServer)shuffleServers.get(1), 1000);
    enableTimeout((MockedShuffleServer)shuffleServers.get(2), 1000);

    blockIdBitmap = Roaring64NavigableMap.bitmapOf();
    blocks = createShuffleBlockList(
      0, 0, 0, 3, 25, blockIdBitmap,
      expectedData, Lists.newArrayList(shuffleServerInfo1, shuffleServerInfo2, shuffleServerInfo3));
    result = shuffleWriteClientImpl.sendShuffleData(testAppId, blocks);
    failedBlockIdBitmap = Roaring64NavigableMap.bitmapOf();
    succBlockIdBitmap = Roaring64NavigableMap.bitmapOf();
    for (Long blockId : result.getSuccessBlockIds()) {
      succBlockIdBitmap.addLong(blockId);
    }
    for (Long blockId : result.getFailedBlockIds()) {
      failedBlockIdBitmap.addLong(blockId);
    }
    assertEquals(blockIdBitmap, failedBlockIdBitmap);
    assertEquals(0, succBlockIdBitmap.getLongCardinality());

    // report result should fail
    partitionToBlockIds.put(0, Lists.newArrayList(blockIdBitmap.stream().iterator()));
    try {
      partitionToServers.put(0, Lists.newArrayList(shuffleServerInfo1, shuffleServerInfo2, shuffleServerInfo3));
      shuffleWriteClientImpl.reportShuffleResult(partitionToServers, testAppId, 0, 0L,
        partitionToBlockIds, 1);
      fail();
    } catch (Exception e){
      assertTrue(e.getMessage().startsWith("Report shuffle result is failed"));
    }
    try {
      report = shuffleWriteClientImpl.getShuffleResult("GRPC",
        Sets.newHashSet(shuffleServerInfo1, shuffleServerInfo2, shuffleServerInfo3),
        testAppId, 0, 0);
      fail();
    } catch (Exception e) {
      assertTrue(e.getMessage().startsWith("Get shuffle result is failed"));
    }

    // The client should not read any data, because write is failed
    assertEquals(readClient.readShuffleBlockData(), null);
  }

  protected void validateResult(ShuffleReadClientImpl readClient, Map<Long, byte[]> expectedData,
                                Roaring64NavigableMap blockIdBitmap) {
    CompressedShuffleBlock csb = readClient.readShuffleBlockData();
    Roaring64NavigableMap matched = Roaring64NavigableMap.bitmapOf();
    while (csb != null && csb.getByteBuffer() != null) {
      for (Map.Entry<Long, byte[]> entry : expectedData.entrySet()) {
        if (compareByte(entry.getValue(), csb.getByteBuffer())) {
          matched.addLong(entry.getKey());
          break;
        }
      }
      csb = readClient.readShuffleBlockData();
    }
    assertTrue(blockIdBitmap.equals(matched));
  }
}
