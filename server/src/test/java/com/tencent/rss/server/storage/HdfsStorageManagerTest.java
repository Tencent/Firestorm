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

package com.tencent.rss.server.storage;

import java.util.Map;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.tencent.rss.common.RemoteStorageInfo;
import com.tencent.rss.server.ShuffleServerConf;
import com.tencent.rss.server.ShuffleServerMetrics;
import com.tencent.rss.storage.common.HdfsStorage;
import com.tencent.rss.storage.util.StorageType;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class HdfsStorageManagerTest {

  @Before
  public void prepare() {
    ShuffleServerMetrics.register();
  }

  @After
  public void clear() {
    ShuffleServerMetrics.clear();
  }

  @Test
  public void testRegisterRemoteStorage() {
    ShuffleServerConf conf = new ShuffleServerConf();
    conf.setLong(ShuffleServerConf.FLUSH_COLD_STORAGE_THRESHOLD_SIZE, 2000L);
    conf.setString(ShuffleServerConf.RSS_STORAGE_BASE_PATH, "test");
    conf.setLong(ShuffleServerConf.DISK_CAPACITY, 1024L);
    conf.setString(ShuffleServerConf.RSS_STORAGE_TYPE, StorageType.MEMORY_LOCALFILE_HDFS.name());
    HdfsStorageManager hdfsStorageManager = new HdfsStorageManager(conf);
    final String remoteStoragePath1 = "hdfs://path1";
    final String remoteStoragePath2 = "hdfs://path2";
    final String remoteStoragePath3 = "hdfs://path3";
    hdfsStorageManager.registerRemoteStorage(
        "app1",
        new RemoteStorageInfo("hdfs://path1", ImmutableMap.of("k1", "v1", "k2", "v2")));
    hdfsStorageManager.registerRemoteStorage(
        "app2",
        new RemoteStorageInfo("hdfs://path2", ImmutableMap.of("k3", "v3")));
    hdfsStorageManager.registerRemoteStorage(
        "app3",
        new RemoteStorageInfo("hdfs://path3", Maps.newHashMap()));
    Map<String, HdfsStorage> appStorageMap =  hdfsStorageManager.getAppIdToStorages();
    assertEquals(3, appStorageMap.size());
    assertEquals(Sets.newHashSet("app1", "app2", "app3"), appStorageMap.keySet());
    HdfsStorage hs1 = hdfsStorageManager.getAppIdToStorages().get("app1");
    assertEquals("v1", hs1.getConf().get("k1"));
    assertEquals("v2", hs1.getConf().get("k2"));
    assertNull(hs1.getConf().get("k3"));
    HdfsStorage hs2 = hdfsStorageManager.getAppIdToStorages().get("app2");
    assertEquals("v3", hs2.getConf().get("k3"));
    assertNull(hs2.getConf().get("k1"));
    assertNull(hs2.getConf().get("k2"));
    HdfsStorage hs3 = hdfsStorageManager.getAppIdToStorages().get("app3");
    assertNull(hs3.getConf().get("k1"));
    assertNull(hs3.getConf().get("k2"));
    assertNull(hs3.getConf().get("k3"));
  }
}

