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

package com.tencent.rss.coordinator;

import java.io.File;
import java.io.FileWriter;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.Map;
import java.util.Objects;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;

import com.tencent.rss.storage.HdfsTestBase;

import static java.lang.Thread.sleep;
import static org.junit.Assert.assertEquals;

public class ClientConfManagerTest extends HdfsTestBase {

  @Before
  public void setUp() {
    CoordinatorMetrics.register();
  }

  @Test
  public void testLocal() throws Exception {
    File cfgFile = tmpDir.newFile();
    FileWriter fileWriter = new FileWriter(cfgFile);
    PrintWriter printWriter = new PrintWriter(fileWriter);
    printWriter.println("spark.mock.1 abc");
    printWriter.println(" spark.mock.2   123 ");
    printWriter.println("spark.mock.3 true  ");
    printWriter.flush();
    printWriter.close();
    final String filePath = Objects.requireNonNull(
        getClass().getClassLoader().getResource("coordinator.conf")).getFile();
    CoordinatorConf conf = new CoordinatorConf(filePath);
    conf.set(CoordinatorConf.COORDINATOR_DYNAMIC_CLIENT_CONF_PATH, cfgFile.toURI().toString());
    ClientConfManager clientConfManager = new ClientConfManager(conf, HdfsTestBase.conf);
    sleep(1200);
    Map<String, String> clientConf = clientConfManager.getClientConf();
    assertEquals("abc", clientConf.get("spark.mock.1"));
    assertEquals("123", clientConf.get("spark.mock.2"));
    assertEquals("true", clientConf.get("spark.mock.3"));
    assertEquals(3, clientConf.size());

    fileWriter = new FileWriter(cfgFile);
    printWriter = new PrintWriter(fileWriter);
    printWriter.println("spark.mock.4 deadbeaf");
    printWriter.println("spark.mock.5 9527");
    printWriter.close();
    sleep(1200);
    clientConf = clientConfManager.getClientConf();
    assertEquals("deadbeaf", clientConf.get("spark.mock.4"));
    assertEquals("9527", clientConf.get("spark.mock.5"));
    assertEquals(2, clientConf.size());
    clientConfManager.close();
  }

  @Test
  public void testHDFS() throws Exception {
    String cfgFile = HDFS_URI + "/test/client_conf";
    Path path = new Path(cfgFile);
    FSDataOutputStream out = fs.create(path);
    PrintWriter printWriter = new PrintWriter(new OutputStreamWriter(out));
    printWriter.println("spark.mock.1 abc");
    printWriter.println(" spark.mock.2   123 ");
    printWriter.println("spark.mock.3 true  ");
    printWriter.flush();
    printWriter.close();

    final String filePath = Objects.requireNonNull(
        getClass().getClassLoader().getResource("coordinator.conf")).getFile();
    CoordinatorConf conf = new CoordinatorConf(filePath);
    conf.set(CoordinatorConf.COORDINATOR_DYNAMIC_CLIENT_CONF_PATH, cfgFile);

    ClientConfManager clientConfManager = new ClientConfManager(conf, HdfsTestBase.conf);
    sleep(1200);
    Map<String, String> clientConf = clientConfManager.getClientConf();
    assertEquals("abc", clientConf.get("spark.mock.1"));
    assertEquals("123", clientConf.get("spark.mock.2"));
    assertEquals("true", clientConf.get("spark.mock.3"));
    assertEquals(3, clientConf.size());

    out = fs.create(path);
    printWriter = new PrintWriter(new OutputStreamWriter(out));
    printWriter.println("spark.mock.4 deadbeaf");
    printWriter.println("spark.mock.5 9527");
    printWriter.close();
    sleep(1200);
    clientConf = clientConfManager.getClientConf();
    assertEquals("deadbeaf", clientConf.get("spark.mock.4"));
    assertEquals("9527", clientConf.get("spark.mock.5"));
    assertEquals(2, clientConf.size());
    clientConfManager.close();
  }
}
