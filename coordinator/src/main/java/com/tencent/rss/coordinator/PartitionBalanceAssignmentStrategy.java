/*
 * Tencent is pleased to support the open source community by making
 * Firestorm-Spark remote shuffle server available.

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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.tencent.rss.common.PartitionRange;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

public class PartitionBalanceAssignmentStrategy implements AssignmentStrategy {

  private ClusterManager clusterManager;
  private Map<ServerNode, PartitionInfo> serverToPartitions = Maps.newConcurrentMap();

  public PartitionBalanceAssignmentStrategy(ClusterManager clusterManager) {
    this.clusterManager = clusterManager;
  }

  @Override
  public PartitionRangeAssignment assign(
      int totalPartitionNum,
      int partitionNumPerRange,
      int replica,
      Set<String> requiredTags) {

    if (partitionNumPerRange != 1) {
      throw new RuntimeException("PartitionNumPerRange must be one");
    }

    SortedMap<PartitionRange, List<ServerNode>> assignments = new TreeMap<>();
    synchronized (this) {
        List<ServerNode> nodes = clusterManager.getServerList(requiredTags);
        Map<ServerNode, PartitionInfo> newPartitionInfos = Maps.newConcurrentMap();
        for (ServerNode node : nodes) {
          PartitionInfo partitionInfo;
          if (serverToPartitions.containsKey(node)) {
            partitionInfo = serverToPartitions.get(node);
            if (partitionInfo.getTimestamp() < node.getTimestamp()) {
              partitionInfo.resetPartitionNum();
              partitionInfo.setTimestamp(node.getTimestamp());
            }
          } else {
            partitionInfo = new PartitionInfo();
          }
          newPartitionInfos.putIfAbsent(node, partitionInfo);
        }
        serverToPartitions = newPartitionInfos;
        int assignPartitions = totalPartitionNum * replica / clusterManager.getShuffleNodesMax();
        for (ServerNode node : nodes) {
          int p = serverToPartitions.get(node).getPartitionNum();
        }
        nodes.sort(new Comparator<ServerNode>() {
          @Override
          public int compare(ServerNode o1, ServerNode o2) {
            PartitionInfo partitionInfo1 = serverToPartitions.get(o1);
            PartitionInfo partitionInfo2 = serverToPartitions.get(o2);
            double v1 = o1.getAvailableMemory() * 1.0 / (partitionInfo1.getPartitionNum() + assignPartitions);
            double v2 = o2.getAvailableMemory() * 1.0 / (partitionInfo2.getPartitionNum() + assignPartitions);
            return -Double.compare(v1, v2);
          }
        });
        List<ServerNode> assignedNodes = nodes.subList(0, clusterManager.getShuffleNodesMax());
        int idx = 0;
        for (int partition = 0; partition < totalPartitionNum; partition++) {
          List<ServerNode> rangeNodes = Lists.newArrayList();
          for (int rc = 0; rc < replica; rc++) {
            idx = nextIdx(idx, assignedNodes.size());
            ServerNode node = assignedNodes.get(idx);
            serverToPartitions.get(node).incrementPartitionNum();;
            rangeNodes.add(node);
          }
          assignments.put(new PartitionRange(partition, partition), rangeNodes);
        }
    }
    return new PartitionRangeAssignment(assignments);
  }

  @VisibleForTesting
  Map<ServerNode, PartitionInfo> getServerToPartitions() {
    return serverToPartitions;
  }

  @VisibleForTesting
  int nextIdx(int idx, int size) {
    ++idx;
    if (idx >= size) {
      idx = 0;
    }
    return idx;
  }

  class PartitionInfo {

    PartitionInfo() {
      partitionNum = 0;
      timestamp = System.currentTimeMillis();
    }

    int partitionNum;
    long timestamp;

    public int getPartitionNum() {
      return partitionNum;
    }

    public void resetPartitionNum() {
      this.partitionNum = 0;
    }

    public void incrementPartitionNum() {
      partitionNum++;
    }

    public long getTimestamp() {
      return timestamp;
    }

    public void setTimestamp(long timestamp) {
      this.timestamp = timestamp;
    }
  }
}
