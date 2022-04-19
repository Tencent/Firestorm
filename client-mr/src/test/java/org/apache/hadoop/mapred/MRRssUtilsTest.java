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

package org.apache.hadoop.mapred;

import org.apache.hadoop.mapreduce.TaskType;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class MRRssUtilsTest {

  @Test
  public void TaskAttemptIdTest() {
    long taskAttemptId = 0x1000ad12;
    TaskAttemptID mrTaskAttemptId = MRRssUtils.createMRTaskAttemptId(new JobID(), TaskType.MAP, taskAttemptId);
    long testId = MRRssUtils.convertTaskAttemptIdToLong(mrTaskAttemptId);
    assertEquals(taskAttemptId, testId);
    taskAttemptId = 0xff1000ad12L;
    mrTaskAttemptId = MRRssUtils.createMRTaskAttemptId(new JobID(), TaskType.MAP, taskAttemptId);
    testId = MRRssUtils.convertTaskAttemptIdToLong(mrTaskAttemptId);
    assertEquals(taskAttemptId, testId);
  }
}
