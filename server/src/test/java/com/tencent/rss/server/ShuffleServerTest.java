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

package com.tencent.rss.server;

import com.tencent.rss.common.util.ExitUtils;
import com.tencent.rss.common.util.ExitUtils.ExitException;
import com.tencent.rss.storage.util.StorageType;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class ShuffleServerTest {

  @Test
  public void startTest() {
    try {
      ShuffleServerConf serverConf = new ShuffleServerConf();
      serverConf.setInteger(ShuffleServerConf.RPC_SERVER_PORT, 9527);
      serverConf.setString(ShuffleServerConf.RSS_STORAGE_TYPE, StorageType.MEMORY_LOCALFILE.name());
      serverConf.setInteger(ShuffleServerConf.JETTY_HTTP_PORT, 9528);
      serverConf.setString(ShuffleServerConf.RSS_COORDINATOR_QUORUM, "localhost:0");
      serverConf.setString(ShuffleServerConf.RSS_STORAGE_BASE_PATH, "/tmp/null");
      serverConf.setLong(ShuffleServerConf.DISK_CAPACITY, 1024L * 1024L * 1024L);
      serverConf.setLong(ShuffleServerConf.SERVER_BUFFER_CAPACITY, 100);
      serverConf.setLong(ShuffleServerConf.SERVER_READ_BUFFER_CAPACITY, 10);

      ShuffleServer ss1 = new ShuffleServer(serverConf);
      ss1.start();

      ExitUtils.disableSystemExit();
      ShuffleServer ss2 = new ShuffleServer(serverConf);
      String expectMessage = "Fail to start jetty http server";
      final int expectStatus = 1;
      try {
        ss2.start();
      } catch (Exception e) {
        assertEquals(expectMessage, e.getMessage());
        assertEquals(expectStatus, ((ExitException) e).getStatus());
      }

      serverConf.setInteger("rss.jetty.http.port", 9529);
      ss2 = new ShuffleServer(serverConf);
      expectMessage = "Fail to start grpc server";
      try {
        ss2.start();
      } catch (Exception e) {
        assertEquals(expectMessage, e.getMessage());
        assertEquals(expectStatus, ((ExitException) e).getStatus());
      }

      final Thread t = new Thread(null, () -> {
        throw new AssertionError("TestUncaughtException");
      }, "testThread");
      t.start();
      t.join();
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }

  }
}
