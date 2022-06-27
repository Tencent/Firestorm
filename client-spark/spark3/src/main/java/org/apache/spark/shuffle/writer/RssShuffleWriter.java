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

package org.apache.spark.shuffle.writer;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Uninterruptibles;
import org.apache.spark.Partitioner;
import org.apache.spark.ShuffleDependency;
import org.apache.spark.SparkConf;
import org.apache.spark.executor.ShuffleWriteMetrics;
import org.apache.spark.memory.TaskMemoryManager;
import org.apache.spark.scheduler.MapStatus;
import org.apache.spark.shuffle.MemoryLimitedMap;
import org.apache.spark.shuffle.RssShuffleHandle;
import org.apache.spark.shuffle.RssShuffleManager;
import org.apache.spark.shuffle.RssSparkConfig;
import org.apache.spark.shuffle.ShuffleWriter;
import org.apache.spark.storage.BlockManagerId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Function1;
import scala.Function2;
import scala.Option;
import scala.Product2;
import scala.Tuple2;
import scala.collection.Iterator;
import scala.runtime.AbstractFunction1;
import scala.runtime.AbstractFunction2;

import com.tencent.rss.client.api.ShuffleWriteClient;
import com.tencent.rss.common.ShuffleBlockInfo;
import com.tencent.rss.common.ShuffleServerInfo;
import com.tencent.rss.common.exception.RssException;
import com.tencent.rss.storage.util.StorageType;

public class RssShuffleWriter<K, V, C> extends ShuffleWriter<K, V> {

  private static final Logger LOG = LoggerFactory.getLogger(RssShuffleWriter.class);
  private static final String DUMMY_HOST = "dummy_host";
  private static final int DUMMY_PORT = 99999;

  private final String appId;
  private final int shuffleId;
  private final WriteBufferManager bufferManager;
  private final String taskId;
  private final long taskAttemptId;
  private final int numMaps;
  private final ShuffleDependency<K, V, C> shuffleDependency;
  private final ShuffleWriteMetrics shuffleWriteMetrics;
  private final Partitioner partitioner;
  private final RssShuffleManager shuffleManager;
  private final boolean shouldPartition;
  private final long sendCheckTimeout;
  private final long sendCheckInterval;
  private final long sendSizeLimit;
  private final int bitmapSplitNum;
  private final Map<Integer, Set<Long>> partitionToBlockIds;
  private final ShuffleWriteClient shuffleWriteClient;
  private final Map<Integer, List<ShuffleServerInfo>> partitionToServers;
  private final Set shuffleServersForData;
  private final long[] partitionLengths;
  private boolean isMemoryShuffleEnabled;
  private boolean isMapsideMergeEnabled;
  private final long mapsideMergeSpillThreshold;
  private final TaskMemoryManager taskMemoryManager;

  public RssShuffleWriter(
      String appId,
      int shuffleId,
      String taskId,
      long taskAttemptId,
      WriteBufferManager bufferManager,
      ShuffleWriteMetrics shuffleWriteMetrics,
      RssShuffleManager shuffleManager,
      SparkConf sparkConf,
      ShuffleWriteClient shuffleWriteClient,
      RssShuffleHandle rssHandle, TaskMemoryManager taskMemoryManager) {
    LOG.warn("RssShuffle start write taskAttemptId data" + taskAttemptId);
    this.shuffleManager = shuffleManager;
    this.appId = appId;
    this.bufferManager = bufferManager;
    this.shuffleId = shuffleId;
    this.taskId = taskId;
    this.taskAttemptId = taskAttemptId;
    this.numMaps = rssHandle.getNumMaps();
    this.shuffleWriteMetrics = shuffleWriteMetrics;
    this.shuffleDependency = rssHandle.getDependency();
    this.partitioner = shuffleDependency.partitioner();
    this.shouldPartition = partitioner.numPartitions() > 1;
    this.sendCheckInterval = sparkConf.getLong(RssSparkConfig.RSS_CLIENT_SEND_CHECK_INTERVAL_MS,
        RssSparkConfig.RSS_CLIENT_SEND_CHECK_INTERVAL_MS_DEFAULT_VALUE);
    this.sendCheckTimeout = sparkConf.getLong(RssSparkConfig.RSS_CLIENT_SEND_CHECK_TIMEOUT_MS,
        RssSparkConfig.RSS_CLIENT_SEND_CHECK_TIMEOUT_MS_DEFAULT_VALUE);
    this.sendSizeLimit = sparkConf.getSizeAsBytes(RssSparkConfig.RSS_CLIENT_SEND_SIZE_LIMIT,
        RssSparkConfig.RSS_CLIENT_SEND_SIZE_LIMIT_DEFAULT_VALUE);
    this.bitmapSplitNum = sparkConf.getInt(RssSparkConfig.RSS_CLIENT_BITMAP_SPLIT_NUM,
        RssSparkConfig.RSS_CLIENT_BITMAP_SPLIT_NUM_DEFAULT_VALUE);
    this.partitionToBlockIds = Maps.newConcurrentMap();
    this.shuffleWriteClient = shuffleWriteClient;
    this.shuffleServersForData = rssHandle.getShuffleServersForData();
    this.partitionLengths = new long[partitioner.numPartitions()];
    Arrays.fill(partitionLengths, 0);
    partitionToServers = rssHandle.getPartitionToServers();
    this.isMemoryShuffleEnabled = isMemoryShuffleEnabled(
        sparkConf.get(RssSparkConfig.RSS_STORAGE_TYPE));
    this.isMapsideMergeEnabled = sparkConf.getBoolean(RssSparkConfig.RSS_CLIENT_MAPSIDE_MERGE_ENABLE,
        RssSparkConfig.RSS_CLIENT_MEGE_ENABLE_DEFAULT_VALUE);
    this.mapsideMergeSpillThreshold = sparkConf.getSizeAsBytes(
        RssSparkConfig.RSS_CLIENT_MAPSIDE_MERGE_SPILL_THRESHOLD,
        RssSparkConfig.RSS_CLIENT_MAPSIDE_MERGE_SPILL_THRESHOLD_DEFAULT_VALUE);
    this.taskMemoryManager = taskMemoryManager;
  }

  private boolean isMemoryShuffleEnabled(String storageType) {
    return StorageType.MEMORY_LOCALFILE.name().equals(storageType)
        || StorageType.MEMORY_HDFS.name().equals(storageType)
        || StorageType.MEMORY_LOCALFILE_HDFS.name().equals(storageType);
  }

  @Override
  public void write(Iterator<Product2<K, V>> records) throws IOException {
    List<ShuffleBlockInfo> shuffleBlockInfos = null;
    Set<Long> blockIds = Sets.newConcurrentHashSet();

    Function1<Tuple2<K, V>, Void> spillFunc = new AbstractFunction1<Tuple2<K, V>, Void>() {
      @Override
      public Void apply(Tuple2<K, V> tuple) {
        int partition = getPartition(tuple._1);
        List<ShuffleBlockInfo> shuffleBlockInfos = bufferManager.addRecord(partition, tuple._1, tuple._2);
        if (shuffleBlockInfos != null && !shuffleBlockInfos.isEmpty()) {
          processShuffleBlockInfos(shuffleBlockInfos, blockIds);
        }
        return null;
      }
    };

    MemoryLimitedMap<K, V> memoryLimitedMap = new MemoryLimitedMap<>(
            taskMemoryManager, mapsideMergeSpillThreshold, spillFunc
    );

    while (records.hasNext()) {
      Product2<K, V> record = records.next();
      if (shuffleDependency.mapSideCombine()) {
        final Function1 createCombiner = shuffleDependency.aggregator().get().createCombiner();
        if (isMapsideMergeEnabled) {
          final Function2 mergeValues = shuffleDependency.aggregator().get().mergeValue();
          Function2 updateFunc = new AbstractFunction2<Boolean, V, V>() {
            @Override
            public V apply(Boolean hasKey, V oldVal) {
              if (hasKey) {
                return (V) mergeValues.apply(oldVal, record._2());
              }
              return (V) createCombiner.apply(record._2());
            }
          };
          memoryLimitedMap.changeValue(record._1(), updateFunc);
        } else {
          int partition = getPartition(record._1());
          shuffleBlockInfos = bufferManager.addRecord(partition, record._1(), createCombiner.apply(record._2()));
        }
      } else {
        int partition = getPartition(record._1());
        shuffleBlockInfos = bufferManager.addRecord(partition, record._1(), record._2());
      }
      if (shuffleBlockInfos != null && !shuffleBlockInfos.isEmpty()) {
        processShuffleBlockInfos(shuffleBlockInfos, blockIds);
      }
    }

    // If merge enable, add all the rest records from map to buffer manager.
    if (!memoryLimitedMap.isEmpty()) {
      Iterator<Tuple2<K,V>> iter = memoryLimitedMap.iterator();
      while (iter.hasNext()) {
        spillFunc.apply(iter.next());
      }
    }

    final long start = System.currentTimeMillis();
    shuffleBlockInfos = bufferManager.clear();
    if (shuffleBlockInfos != null && !shuffleBlockInfos.isEmpty()) {
      processShuffleBlockInfos(shuffleBlockInfos, blockIds);
    }
    long checkStartTs = System.currentTimeMillis();
    checkBlockSendResult(blockIds);
    long commitStartTs = System.currentTimeMillis();
    long checkDuration = commitStartTs - checkStartTs;
    if (!isMemoryShuffleEnabled) {
      sendCommit();
    }
    long writeDurationMs = bufferManager.getWriteTime() + (System.currentTimeMillis() - start);
    shuffleWriteMetrics.incWriteTime(TimeUnit.MILLISECONDS.toNanos(writeDurationMs));
    LOG.info("Finish write shuffle for appId[" + appId + "], shuffleId[" + shuffleId
        + "], taskId[" + taskId + "] with write " + writeDurationMs + " ms, include checkSendResult["
        + checkDuration + "], commit[" + (System.currentTimeMillis() - commitStartTs) + "], "
        + bufferManager.getManagerCostInfo());
  }

  // only push-based shuffle use this interface, but rss won't be used when push-based shuffle is enabled.
  public long[] getPartitionLengths() {
    return new long[0];
  }

  private void processShuffleBlockInfos(List<ShuffleBlockInfo> shuffleBlockInfoList, Set<Long> blockIds) {
    if (shuffleBlockInfoList != null && !shuffleBlockInfoList.isEmpty()) {
      shuffleBlockInfoList.forEach(sbi -> {
        long blockId = sbi.getBlockId();
        // add blockId to set, check if it is send later
        blockIds.add(blockId);
        // update [partition, blockIds], it will be sent to shuffle server
        int partitionId = sbi.getPartitionId();
        partitionToBlockIds.putIfAbsent(partitionId, Sets.newConcurrentHashSet());
        partitionToBlockIds.get(partitionId).add(blockId);
        partitionLengths[partitionId] += sbi.getLength();
      });
      postBlockEvent(shuffleBlockInfoList);
    }
  }

  protected void postBlockEvent(List<ShuffleBlockInfo> shuffleBlockInfoList) {
    long totalSize = 0;
    List<ShuffleBlockInfo> shuffleBlockInfosPerEvent = Lists.newArrayList();
    for (ShuffleBlockInfo sbi : shuffleBlockInfoList) {
      totalSize += sbi.getSize();
      shuffleBlockInfosPerEvent.add(sbi);
      // split shuffle data according to the size
      if (totalSize > sendSizeLimit) {
        LOG.info("Post event to queue with " + shuffleBlockInfosPerEvent.size()
            + " blocks and " + totalSize + " bytes");
        shuffleManager.postEvent(
            new AddBlockEvent(taskId, shuffleBlockInfosPerEvent));
        shuffleBlockInfosPerEvent = Lists.newArrayList();
        totalSize = 0;
      }
    }
    if (!shuffleBlockInfosPerEvent.isEmpty()) {
      LOG.info("Post event to queue with " + shuffleBlockInfosPerEvent.size()
          + " blocks and " + totalSize + " bytes");
      shuffleManager.postEvent(
          new AddBlockEvent(taskId, shuffleBlockInfosPerEvent));
    }
  }

  @VisibleForTesting
  protected void checkBlockSendResult(Set<Long> blockIds) throws RuntimeException {
    long start = System.currentTimeMillis();
    while (true) {
      Set<Long> successBlockIds = shuffleManager.getSuccessBlockIds(taskId);
      Set<Long> failedBlockIds = shuffleManager.getFailedBlockIds(taskId);

      if (!failedBlockIds.isEmpty()) {
        String errorMsg = "Send failed: Task[" + taskId + "]"
            + " failed because " + failedBlockIds.size()
            + " blocks can't be sent to shuffle server.";
        LOG.error(errorMsg);
        throw new RssException(errorMsg);
      }

      blockIds.removeAll(successBlockIds);
      if (blockIds.isEmpty()) {
        break;
      }
      LOG.info("Wait " + blockIds.size() + " blocks sent to shuffle server");
      Uninterruptibles.sleepUninterruptibly(sendCheckInterval, TimeUnit.MILLISECONDS);
      if (System.currentTimeMillis() - start > sendCheckTimeout) {
        String errorMsg = "Timeout: Task[" + taskId + "] failed because " + blockIds.size()
            + " blocks can't be sent to shuffle server in " + sendCheckTimeout + " ms.";
        LOG.error(errorMsg);
        throw new RssException(errorMsg);
      }
    }
  }

  @VisibleForTesting
  protected void sendCommit() {
    ExecutorService executor = Executors.newSingleThreadExecutor();
    Future<Boolean> future = executor.submit(
        () -> shuffleWriteClient.sendCommit(shuffleServersForData, appId, shuffleId, numMaps));
    int maxWait = 5000;
    int currentWait = 200;
    long start = System.currentTimeMillis();
    while (!future.isDone()) {
      LOG.info("Wait commit to shuffle server for task[" + taskAttemptId + "] cost "
          + (System.currentTimeMillis() - start) + " ms");
      Uninterruptibles.sleepUninterruptibly(currentWait, TimeUnit.MILLISECONDS);
      currentWait = Math.min(currentWait * 2, maxWait);
    }
    try {
      if (!future.get()) {
        throw new RssException("Failed to commit task to shuffle server");
      }
    } catch (InterruptedException ie) {
      LOG.warn("Ignore the InterruptedException which should be caused by internal killed");
    } catch (Exception e) {
      throw new RuntimeException("Exception happened when get commit status", e);
    } finally {
      executor.shutdown();
    }
  }

  @VisibleForTesting
  protected <K> int getPartition(K key) {
    int result = 0;
    if (shouldPartition) {
      result = partitioner.getPartition(key);
    }
    return result;
  }

  @Override
  public Option<MapStatus> stop(boolean success) {
    try {
      if (success) {
        Map<Integer, List<Long>> ptb = Maps.newHashMap();
        for (Map.Entry<Integer, Set<Long>> entry : partitionToBlockIds.entrySet()) {
          ptb.put(entry.getKey(), Lists.newArrayList(entry.getValue()));
        }
        long start = System.currentTimeMillis();
        shuffleWriteClient.reportShuffleResult(partitionToServers, appId, shuffleId,
            taskAttemptId, ptb, bitmapSplitNum);
        LOG.info("Report shuffle result for task[{}] with bitmapNum[{}] cost {} ms",
            taskAttemptId, bitmapSplitNum, (System.currentTimeMillis() - start));
        // todo: we can replace the dummy host and port with the real shuffle server which we prefer to read
        final BlockManagerId blockManagerId = BlockManagerId.apply(appId + "_" + taskId,
            DUMMY_HOST,
            DUMMY_PORT,
            Option.apply(Long.toString(taskAttemptId)));
        MapStatus mapStatus = MapStatus.apply(blockManagerId, partitionLengths, taskAttemptId);
        return Option.apply(mapStatus);
      } else {
        return Option.empty();
      }
    } finally {
      // free all memory & metadata, or memory leak happen in executor
      if (bufferManager != null) {
        bufferManager.freeAllMemory();
      }
      if (shuffleManager != null) {
        shuffleManager.clearTaskMeta(taskId);
      }
    }
  }

  @VisibleForTesting
  Map<Integer, Set<Long>> getPartitionToBlockIds() {
    return partitionToBlockIds;
  }
}
