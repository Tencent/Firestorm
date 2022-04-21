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

package org.apache.hadoop.mapreduce.task.reduce;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapTaskCompletionEventsUpdate;
import org.apache.hadoop.mapred.TaskCompletionEvent;
import org.apache.hadoop.mapred.TaskUmbilicalProtocol;
import org.apache.hadoop.mapreduce.MRRssUtils;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

import com.tencent.rss.common.exception.RssException;

public class RssEventFetcher<K,V> {
  private static final long SLEEP_TIME = 1000;
  private static final int MAX_RETRIES = 10;
  private static final int RETRY_PERIOD = 5000;
  private static final Log LOG = LogFactory.getLog(RssEventFetcher.class);

  private final TaskAttemptID reduce;
  private final TaskUmbilicalProtocol umbilical;
  private int fromEventIdx = 0;
  private final int maxEventsToFetch;
  private final ExceptionReporter exceptionReporter;
  private JobConf jobConf;
  private volatile boolean stopped = false;

  private Set<TaskAttemptID> successMaps = new HashSet<TaskAttemptID>();
  private Set<TaskAttemptID> obsoleteMaps = new HashSet<TaskAttemptID>();
  private int tipFailedCount = 0;
  private final int totalMapsCount;

  public RssEventFetcher(TaskAttemptID reduce,
                         TaskUmbilicalProtocol umbilical,
                         JobConf jobConf,
                         ExceptionReporter reporter,
                         int maxEventsToFetch) {
    this.jobConf = jobConf;
    this.totalMapsCount = jobConf.getNumMapTasks();
    this.reduce = reduce;
    this.umbilical = umbilical;
    exceptionReporter = reporter;
    this.maxEventsToFetch = maxEventsToFetch;
  }

  public Roaring64NavigableMap fetchAllRssTaskIds() {
    try {
      acceptMapCompletionEvents();
    } catch (Exception e) {
      exceptionReporter.reportException(
        new RssException("Reduce: " + reduce
        + " fails to accept completion events due to: "
        + e.getMessage())
      );
      return null;
    }

    Roaring64NavigableMap taskIds = Roaring64NavigableMap.bitmapOf();
    for (TaskAttemptID mapTask: successMaps) {
      if (!obsoleteMaps.contains(mapTask)) {
        long taskId = MRRssUtils.convertTaskAttemptIdToLong(mapTask);
        taskIds.addLong(taskId);
      }
    }
    if (taskIds.getLongCardinality() + tipFailedCount != totalMapsCount) {
      exceptionReporter.reportException(
        new IllegalStateException("TaskAttemptIDs are inconsistent with total map tasks")
      );
      return null;
    }
    return taskIds;
  }

  public void resolve(TaskCompletionEvent event) {
    // Process the TaskCompletionEvents:
    // 1. Save the SUCCEEDED maps in knownOutputs to fetch the outputs.
    // 2. Save the OBSOLETE/FAILED/KILLED maps in obsoleteOutputs to stop
    //    fetching from those maps.
    // 3. Remove TIPFAILED maps from neededOutputs since we don't need their
    //    outputs at all.
    successMaps.contains(event);
    switch (event.getTaskStatus()) {
      case SUCCEEDED:
        successMaps.add(event.getTaskAttemptId());
        break;

      case FAILED:
      case KILLED:
      case OBSOLETE:
        obsoleteMaps.add(event.getTaskAttemptId());
        LOG.info("Ignoring obsolete output of "
          + event.getTaskStatus() + " map-task: '" + event.getTaskAttemptId() + "'");
        break;

      case TIPFAILED:
        tipFailedCount++;
        LOG.info("Ignoring output of failed map TIP: '"
          + event.getTaskAttemptId() + "'");
        break;

      default:
        break;
    }
  }

  // Since slow start is disabled, the reducer can get all completed maps
  public void acceptMapCompletionEvents() throws IOException {

    TaskCompletionEvent[] events = null;

    do {
      MapTaskCompletionEventsUpdate update =
        umbilical.getMapCompletionEvents(
          (org.apache.hadoop.mapred.JobID) reduce.getJobID(),
          fromEventIdx,
          maxEventsToFetch,
          (org.apache.hadoop.mapred.TaskAttemptID) reduce);
      events = update.getMapTaskCompletionEvents();
      LOG.debug("Got " + events.length + " map completion events from "
        + fromEventIdx);

      assert !update.shouldReset() : "Unexpected legacy state";

      // Update the last seen event ID
      fromEventIdx += events.length;

      for (TaskCompletionEvent event : events) {
        resolve(event);
      }
    } while (events.length == maxEventsToFetch);
  }
}
