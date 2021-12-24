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

package com.tencent.rss.storage.common;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.roaringbitmap.RoaringBitmap;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;


public class LocalStorageTest {

  @ClassRule
  public static final TemporaryFolder tmpDir = new TemporaryFolder();
  private static File testBaseDir;

  @BeforeClass
  public static void setUp() throws IOException  {
    testBaseDir = tmpDir.newFolder("test");
  }

  @AfterClass
  public static void tearDown() {
    tmpDir.delete();
  }

  @Ignore
  @Test
  public void cleanTest() {
    try {
      LocalStorage item = LocalStorage.newBuilder().basePath(testBaseDir.getAbsolutePath())
          .cleanupThreshold(50)
          .highWaterMarkOfWrite(100)
          .lowWaterMarkOfWrite(100)
          .capacity(100)
          .cleanIntervalMs(5000)
          .shuffleExpiredTimeoutMs(1)
          .build();
      File baseDir = tmpDir.newFolder(testBaseDir.getName(),"app-1");
      assertTrue(baseDir.exists());
      File dir1 = tmpDir.newFolder(testBaseDir.getName(), "app-1", "1");
      File dir2 = tmpDir.newFolder(testBaseDir.getName(), "app-1", "2");
      assertTrue(dir1.exists());
      assertTrue(dir2.exists());
      item.createMetadataIfNotExist("app-1/1");
      item.createMetadataIfNotExist("app-1/2");
      item.updateWrite("app-1/1", 0, Lists.newArrayList());
      item.updateWrite("app-1/2", 0, Lists.newArrayList());

      assertTrue(dir1.exists());
      assertTrue(dir2.exists());
      item.getMetaData().updateShuffleSize("app-1/1", 25);
      item.getMetaData().updateShuffleSize("app-1/2", 35);
      item.getMetaData().updateDiskSize(60);
      assertEquals(60, item.getMetaData().getDiskSize().get());
      // item.clean();
      assertTrue(dir1.exists());
      assertTrue(dir2.exists());
      item.getMetaData().updateUploadedShuffleSize("app-1/1", 25);
      // item.clean();
      assertTrue(dir1.exists());
      assertTrue(dir2.exists());
      item.getMetaData().prepareStartRead("app-1/1");
      // item.clean();
      assertTrue(dir1.exists());
      item.getMetaData().updateShuffleLastReadTs("app-1/1");
      Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);
      // item.clean();
      assertFalse(dir1.exists());
      assertTrue(dir2.exists());
      item.getMetaData().prepareStartRead("app-1/2");
      // item.clean();
      assertTrue(dir2.exists());
      assertEquals(35, item.getMetaData().getDiskSize().get());
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }

  @Ignore
  @Test
  public void delayCleanTest() {
    LocalStorage item = LocalStorage.newBuilder().basePath(testBaseDir.getAbsolutePath())
        .cleanupThreshold(0)
        .highWaterMarkOfWrite(100)
        .lowWaterMarkOfWrite(100)
        .capacity(100)
        .cleanIntervalMs(1000)
        .shuffleExpiredTimeoutMs(1)
        .build();

    item.createMetadataIfNotExist("key1");
    item.createMetadataIfNotExist("key2");
    item.createMetadataIfNotExist("key3");
    item.updateWrite("key1", 100, Lists.newArrayList());
    item.updateWrite("key2", 50, Lists.newArrayList());
    item.updateWrite("key3", 95, Lists.newArrayList());
    assertEquals(3, item.getMetaData().getShuffleMetaSet().size());
    assertEquals(100, item.getMetaData().getShuffleSize("key1"));
    assertEquals(50, item.getMetaData().getShuffleSize("key2"));
    assertEquals(95, item.getMetaData().getShuffleSize("key3"));
    assertEquals(245, item.getMetaData().getDiskSize().intValue());

    // item.start();
    item.getExpiredShuffleKeys().offer("key1");
    item.getExpiredShuffleKeys().offer("key2");
    assertEquals(2, item.getExpiredShuffleKeys().size());
    Uninterruptibles.sleepUninterruptibly(1200, TimeUnit.MILLISECONDS);
    assertEquals(1, item.getMetaData().getShuffleMetaSet().size());
    assertEquals(95, item.getMetaData().getDiskSize().intValue());
    assertEquals(95, item.getMetaData().getShuffleSize("key3"));
    assertEquals(0, item.getExpiredShuffleKeys().size());

    item.getExpiredShuffleKeys().offer("key3");
    assertEquals(1, item.getExpiredShuffleKeys().size());
    Uninterruptibles.sleepUninterruptibly(1200, TimeUnit.MILLISECONDS);
    assertEquals(0, item.getMetaData().getShuffleMetaSet().size());
    assertEquals(0, item.getMetaData().getDiskSize().intValue());
    assertEquals(0, item.getExpiredShuffleKeys().size());

    item.createMetadataIfNotExist("key1");
    item.createMetadataIfNotExist("key2");
    item.updateWrite("key1", 100, Lists.newArrayList());
    item.updateWrite("key2", 50, Lists.newArrayList());
    assertEquals(100, item.getMetaData().getShuffleSize("key1"));
    assertEquals(50, item.getMetaData().getShuffleSize("key2"));
    assertEquals(150, item.getMetaData().getDiskSize().intValue());

    item.getMetaData().prepareStartRead("key1");
    item.getMetaData().updateShuffleLastReadTs("key1");
    Uninterruptibles.sleepUninterruptibly(1200, TimeUnit.MILLISECONDS);
    item.getExpiredShuffleKeys().offer("key2");
    assertEquals(50, item.getMetaData().getDiskSize().intValue());
    assertEquals(1, item.getMetaData().getShuffleMetaSet().size());
    assertEquals(0, item.getMetaData().getShuffleSize("key1"));
    assertEquals(50, item.getMetaData().getShuffleSize("key2"));

    Uninterruptibles.sleepUninterruptibly(1200, TimeUnit.MILLISECONDS);
    assertEquals(0, item.getMetaData().getShuffleMetaSet().size());
    assertEquals(0, item.getMetaData().getDiskSize().intValue());
    assertEquals(0, item.getExpiredShuffleKeys().size());

  }

  @Test
  public void canWriteTest() {
    try {
      LocalStorage item = LocalStorage.newBuilder().basePath(testBaseDir.getAbsolutePath())
          .cleanupThreshold(50)
          .highWaterMarkOfWrite(95)
          .lowWaterMarkOfWrite(80)
          .capacity(100)
          .cleanIntervalMs(5000)
          .build();

      item.getMetaData().updateDiskSize(20);
      assertTrue(item.canWrite());
      item.getMetaData().updateDiskSize(65);
      assertTrue(item.canWrite());
      item.getMetaData().updateDiskSize(10);
      assertFalse(item.canWrite());
      item.getMetaData().updateDiskSize(-10);
      assertFalse(item.canWrite());
      item.getMetaData().updateDiskSize(-10);
      assertTrue(item.canWrite());
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void removeResourcesTest() {
    try {
      LocalStorage item = prepareDiskItem();
      item.removeResources("1/1");
      assertEquals(50L, item.getMetaData().getDiskSize().get());
      assertEquals(0L, item.getMetaData().getShuffleSize("1/1"));
      assertEquals(50L, item.getMetaData().getShuffleSize("1/2"));
      assertTrue(item.getMetaData().getNotUploadedPartitions("1/1").isEmpty());
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }

  private LocalStorage prepareDiskItem() {
    LocalStorage item = LocalStorage.newBuilder().basePath(testBaseDir.getAbsolutePath())
        .cleanupThreshold(50)
        .highWaterMarkOfWrite(95)
        .lowWaterMarkOfWrite(80)
        .capacity(100)
        .cleanIntervalMs(5000)
        .build();
    RoaringBitmap partitionBitMap = RoaringBitmap.bitmapOf();
    partitionBitMap.add(1);
    partitionBitMap.add(2);
    partitionBitMap.add(1);
    List<Integer> partitionList = Lists.newArrayList(1, 2);
    item.createMetadataIfNotExist("1/1");
    item.createMetadataIfNotExist("1/2");
    item.updateWrite("1/1", 100, partitionList);
    item.updateWrite("1/2", 50, Lists.newArrayList());
    assertEquals(150L, item.getMetaData().getDiskSize().get());
    assertEquals(2, item.getMetaData().getNotUploadedPartitions("1/1").getCardinality());
    assertTrue(partitionBitMap.contains(item.getMetaData().getNotUploadedPartitions("1/1")));
    return item;
  }

  @Test
  public void concurrentRemoveResourcesTest() {
    try {
      LocalStorage item = prepareDiskItem();
      Runnable runnable = () -> item.removeResources("1/1");
      List<Thread> testThreads = Lists.newArrayList(new Thread(runnable), new Thread(runnable), new Thread(runnable));
      testThreads.forEach(Thread::start);
      testThreads.forEach(t -> {
        try {
          t.join();
        } catch (InterruptedException e) {

        }
      });

      assertEquals(50L, item.getMetaData().getDiskSize().get());
      assertEquals(0L, item.getMetaData().getShuffleSize("1/1"));
      assertEquals(50L, item.getMetaData().getShuffleSize("1/2"));
      assertTrue(item.getMetaData().getNotUploadedPartitions("1/1").isEmpty());
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void diskMetaTest() {
    LocalStorage item = LocalStorage.newBuilder().basePath(testBaseDir.getAbsolutePath())
        .cleanupThreshold(50)
        .highWaterMarkOfWrite(95)
        .lowWaterMarkOfWrite(80)
        .capacity(100)
        .cleanIntervalMs(5000)
        .build();
    List<Integer> partitionList1 = Lists.newArrayList(1, 2, 3, 4, 5);
    List<Integer> partitionList2 = Lists.newArrayList(6, 7, 8, 9, 10);
    List<Integer> partitionList3 = Lists.newArrayList(1, 2, 3);
    item.createMetadataIfNotExist("key1");
    item.createMetadataIfNotExist("key2");
    item.updateWrite("key1", 10, partitionList1);
    item.updateWrite("key2", 30, partitionList2);
    item.updateUploadedShuffle("key1", 5, partitionList3);

    assertTrue(item.getNotUploadedPartitions("notKey").isEmpty());
    assertEquals(2, item.getNotUploadedPartitions("key1").getCardinality());
    assertEquals(5, item.getNotUploadedPartitions("key2").getCardinality());
    assertEquals(0, item.getNotUploadedSize("notKey"));
    assertEquals(5, item.getNotUploadedSize("key1"));
    assertEquals(30, item.getNotUploadedSize("key2"));

    assertTrue(item.getSortedShuffleKeys(true, 1).isEmpty());
    assertTrue(item.getSortedShuffleKeys(true, 2).isEmpty());
    item.prepareStartRead("key1");
    assertEquals(1, item.getSortedShuffleKeys(true, 3).size());
    assertEquals(1, item.getSortedShuffleKeys(false, 1).size());
    assertEquals("key2", item.getSortedShuffleKeys(false, 1).get(0));
    assertEquals(2, item.getSortedShuffleKeys(false, 2).size());
    assertEquals(2, item.getSortedShuffleKeys(false, 3).size());
  }
}
