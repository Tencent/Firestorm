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

package com.tencent.rss.common;

import java.util.List;
import java.util.Map;

public class ShuffleAssignmentsInfo {

  private Map<Integer, List<ShuffleServerInfo>> partitionToServers;
  private Map<ShuffleServerInfo, List<PartitionRange>> serverToPartitionRanges;

  public ShuffleAssignmentsInfo(
      Map<Integer, List<ShuffleServerInfo>> partitionToServers,
      Map<ShuffleServerInfo, List<PartitionRange>> serverToPartitionRanges) {
    this.partitionToServers = partitionToServers;
    this.serverToPartitionRanges = serverToPartitionRanges;
  }

  public Map<Integer, List<ShuffleServerInfo>> getPartitionToServers() {
    return partitionToServers;
  }

  public Map<ShuffleServerInfo, List<PartitionRange>> getServerToPartitionRanges() {
    return serverToPartitionRanges;
  }
}
