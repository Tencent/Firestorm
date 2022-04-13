/*
 * Tencent is pleased to support the open source community by making
 * Firestorm-Spark remote shuffle server available
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

package org.apache.hadoop.mapred;

import com.tencent.rss.client.api.ShuffleWriteClient;
import com.tencent.rss.common.ShuffleServerInfo;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.hadoop.io.serializer.Serializer;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskCounter;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class RssMapOutputCollector<K extends Object, V extends Object>
    implements MapOutputCollector<K, V> {

  private JobConf jobConf;
  private Task.TaskReporter reporter;
  private MapTask mapTask;

  private Class<K> keyClass;
  private Class<V> valClass;
;
  private int partitions;
  private SortWriteBufferManager bufferManager;

  @Override
  public void init(Context context) throws IOException, ClassNotFoundException {
    SerializationFactory serializationFactory;
    Counters.Counter mapOutputByteCounter;
    Counters.Counter mapOutputRecordCounter;
    jobConf = context.getJobConf();
    reporter = context.getReporter();
    mapTask = context.getMapTask();
    mapOutputByteCounter = reporter.getCounter(TaskCounter.MAP_OUTPUT_BYTES);
    mapOutputRecordCounter = reporter.getCounter(TaskCounter.MAP_OUTPUT_RECORDS);
    keyClass = (Class<K>)jobConf.getMapOutputKeyClass();
    valClass = (Class<V>)jobConf.getMapOutputValueClass();
    serializationFactory = new SerializationFactory(jobConf);
    Serializer<K> keySerializer = serializationFactory.getSerializer(keyClass);
    Serializer<V> valSerializer = serializationFactory.getSerializer(valClass);
    int sortmb = jobConf.getInt(JobContext.IO_SORT_MB, 100) ;
    if ((sortmb & 0x7FF) != sortmb) {
      throw new IOException(
          "Invalid \"" + JobContext.IO_SORT_MB + "\": " + sortmb);
    }
    long maxMemSize = sortmb << 20;
    partitions = jobConf.getNumReduceTasks();
    long taskAttemptId = (mapTask.getTaskID().getTaskID().getId() << 4) + mapTask.getTaskID().getId();
    int batch;
    RawComparator<K> comparator;
    double memoryThreshold;
    String appId;
    ShuffleWriteClient shuffleWriteClient;
    long sendCheckInterval;
    long sendCheckTimeout;
    Map<Integer, List<ShuffleServerInfo>> partitionToServers;
    bufferManager = new SortWriteBufferManager(
        maxMemSize,
        taskAttemptId,
        batch,
        keySerializer,
        valSerializer,
        comparator,
        memoryThreshold,
        appId,
        shuffleWriteClient,
        sendCheckInterval,
        sendCheckTimeout,
        partitionToServers);
  }

  @Override
  public void collect(K key, V value, int partition) throws IOException, InterruptedException {
    reporter.progress();
    if (key.getClass() != keyClass) {
      throw new IOException("Type mismatch in key from map: expected "
          + keyClass.getName() + ", received "
          + key.getClass().getName());
    }
    if (value.getClass() != valClass) {
      throw new IOException("Type mismatch in value from map: expected "
          + valClass.getName() + ", received "
          + value.getClass().getName());
    }
    if (partition < 0 || partition >= partitions) {
      throw new IOException("Illegal partition for " + key + " (" +
          partition + ")");
    }
    checkRssException();
    bufferManager.addRecord(partition, key, value);
  }

  private void checkRssException() {

  }

  @Override
  public void close() throws IOException, InterruptedException {
    bufferManager.freeAllResources();
  }

  @Override
  public void flush() throws IOException, InterruptedException, ClassNotFoundException {
    bufferManager.waitTriggerFinished();
  }
}
