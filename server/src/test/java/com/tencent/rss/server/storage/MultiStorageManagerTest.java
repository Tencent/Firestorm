package com.tencent.rss.server.storage;

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

import com.google.common.collect.Lists;
import com.tencent.rss.common.ShufflePartitionedBlock;
import com.tencent.rss.server.ShuffleDataFlushEvent;
import com.tencent.rss.server.ShuffleServerConf;
import com.tencent.rss.storage.common.HdfsStorage;
import com.tencent.rss.storage.common.LocalStorage;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertTrue;

public class MultiStorageManagerTest {

  @Test
  public void selectStorageManagerTest() {
    ShuffleServerConf conf = new ShuffleServerConf();
    conf.setLong(ShuffleServerConf.FLUSH_COLD_STORAGE_THRESHOLD_SIZE, 2000L);
    conf.setString(ShuffleServerConf.RSS_STORAGE_BASE_PATH, "test");
    conf.setLong(ShuffleServerConf.DISK_CAPACITY, 1024L * 1024L * 1024L);
    MultiStorageManager manager = new MultiStorageManager(conf, "test");
    List<ShufflePartitionedBlock> blocks = Lists.newArrayList(new ShufflePartitionedBlock(100, 1000, 1, 1, 1L, null));
    ShuffleDataFlushEvent event = new ShuffleDataFlushEvent(
        1, "1", 1, 1, 1, 1000, blocks, null, null);
    assertTrue((manager.selectStorage(event) instanceof LocalStorage));
    event = new ShuffleDataFlushEvent(
        1, "1", 1, 1, 1, 1000000, blocks, null, null);
    assertTrue((manager.selectStorage(event) instanceof HdfsStorage));
    event.increaseRetryTimes();
    assertTrue(manager.selectStorage(event) instanceof LocalStorage);
  }
}
