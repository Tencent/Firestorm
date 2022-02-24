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
import com.google.common.util.concurrent.Uninterruptibles;
import com.tencent.rss.client.impl.ShuffleReadClientImpl;
import com.tencent.rss.client.impl.grpc.ShuffleServerGrpcClient;
import com.tencent.rss.client.request.RssFinishShuffleRequest;
import com.tencent.rss.client.request.RssRegisterShuffleRequest;
import com.tencent.rss.client.request.RssReportShuffleResultRequest;
import com.tencent.rss.client.request.RssSendCommitRequest;
import com.tencent.rss.client.request.RssSendShuffleDataRequest;
import com.tencent.rss.common.PartitionRange;
import com.tencent.rss.common.ShuffleBlockInfo;
import com.tencent.rss.coordinator.CoordinatorConf;
import com.tencent.rss.server.ShuffleServerConf;
import com.tencent.rss.storage.util.StorageType;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class DiskErrorToleranceTest extends ShuffleReadWriteBase {
  private ShuffleServerGrpcClient shuffleServerClient;

  private static File serverTmpDir = Files.createTempDir();
  private static File data1 = new File(serverTmpDir, "data1");
  private static File data2 = new File(serverTmpDir, "data2");

  @BeforeClass
  public static void setupServers() throws Exception {
    serverTmpDir.deleteOnExit();
    CoordinatorConf coordinatorConf = getCoordinatorConf();
    createCoordinatorServer(coordinatorConf);
    ShuffleServerConf shuffleServerConf = getShuffleServerConf();
    shuffleServerConf.setString(ShuffleServerConf.RSS_STORAGE_TYPE, StorageType.LOCALFILE.name());
    shuffleServerConf.setString(ShuffleServerConf.RSS_STORAGE_BASE_PATH,
        data1.getAbsolutePath() + "," + data2.getAbsolutePath());
    shuffleServerConf.setBoolean(ShuffleServerConf.HEALTH_CHECK_ENABLE, true);
    createShuffleServer(shuffleServerConf);
    startServers();
  }

  @Before
  public void createClient() {
    shuffleServerClient = new ShuffleServerGrpcClient(LOCALHOST, SHUFFLE_SERVER_PORT);
  }

  @After
  public void closeClient() {
    shuffleServerClient.close();
  }

  @Test
  public void diskErrorTest() throws Exception {

    String appId = "ap_disk_error_data";
    Map<Long, byte[]> expectedData = Maps.newHashMap();
    Set<Long> expectedBlock1 = Sets.newHashSet();
    Roaring64NavigableMap blockIdBitmap1 = Roaring64NavigableMap.bitmapOf();
    List<ShuffleBlockInfo> blocks1 = createShuffleBlockList(
        0, 0, 1,3, 25, blockIdBitmap1, expectedData);
    RssRegisterShuffleRequest rr1 =  new RssRegisterShuffleRequest(appId, 0,
        Lists.newArrayList(new PartitionRange(0, 0)));
    shuffleServerClient.registerShuffle(rr1);
    blocks1.forEach(b -> expectedBlock1.add(b.getBlockId()));
    Map<Integer, List<ShuffleBlockInfo>> partitionToBlocks = Maps.newHashMap();
    partitionToBlocks.put(0, blocks1);
    Map<Integer, Map<Integer, List<ShuffleBlockInfo>>> shuffleToBlocks = Maps.newHashMap();
    shuffleToBlocks.put(0, partitionToBlocks);
    RssSendShuffleDataRequest rs1 = new RssSendShuffleDataRequest(appId, 3, 1000, shuffleToBlocks);
    shuffleServerClient.sendShuffleData(rs1);
    RssSendCommitRequest rc1 = new RssSendCommitRequest(appId, 0);
    shuffleServerClient.sendCommit(rc1);
    RssFinishShuffleRequest rf1 = new RssFinishShuffleRequest(appId, 0);
    shuffleServerClient.finishShuffle(rf1);

    FileUtils.deleteDirectory(data2);
    assertFalse(data2.exists());
    boolean suc = data2.createNewFile();
    assertTrue(suc);
    Uninterruptibles.sleepUninterruptibly(10, TimeUnit.SECONDS);

    partitionToBlocks.clear();
    shuffleToBlocks.clear();
    Roaring64NavigableMap blockIdBitmap2 = Roaring64NavigableMap.bitmapOf();
    Set<Long> expectedBlock2 = Sets.newHashSet();
    List<ShuffleBlockInfo> blocks2 = createShuffleBlockList(
        0, 0, 2, 5, 30, blockIdBitmap2, expectedData);
    blocks2.forEach(b -> expectedBlock2.add(b.getBlockId()));
    partitionToBlocks.put(0, blocks2);
    shuffleToBlocks.put(0, partitionToBlocks);
    rs1 = new RssSendShuffleDataRequest(appId, 3, 1000, shuffleToBlocks);
    shuffleServerClient.sendShuffleData(rs1);
    shuffleServerClient.sendCommit(rc1);
    shuffleServerClient.finishShuffle(rf1);

    ShuffleReadClientImpl readClient = new ShuffleReadClientImpl(StorageType.LOCALFILE.name(),
        appId, 0, 0, 100, 1, 10, 1000, HDFS_URI + "xxxx",
        blockIdBitmap2, Roaring64NavigableMap.bitmapOf(2), Lists.newArrayList(), conf);
    validateResult(readClient, expectedData);

  }
}
