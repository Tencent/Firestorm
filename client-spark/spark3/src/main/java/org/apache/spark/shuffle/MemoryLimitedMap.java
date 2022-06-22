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

package org.apache.spark.shuffle;

import org.apache.spark.memory.TaskMemoryManager;
import org.apache.spark.util.collection.SizeTrackingAppendOnlyMap;
import org.apache.spark.util.collection.Spillable;
import scala.Function1;
import scala.Function2;
import scala.Tuple2;
import scala.collection.Iterator;

/**
 * This class is the wrapper of {@link SizeTrackingAppendOnlyMap} and to limit the
 * memory usage from the task memory manager. Once the memory usage exceed the
 * {@code spillThresholdSize} or lack the enough memory from task memory manager,
 * it will spill elements by the external function of {@code spillFunc}.
 */
public class MemoryLimitedMap<K, V> extends Spillable<SizeTrackingAppendOnlyMap> {
    private SizeTrackingAppendOnlyMap<K, V> currentMap = new SizeTrackingAppendOnlyMap<>();
    private final long spillThresholdSize;
    private final Function1<Tuple2<K, V>, Void> spillFunc;

    public MemoryLimitedMap(TaskMemoryManager taskMemoryManager, Long spillThresholdSize,
            Function1<Tuple2<K, V>, Void> spillFunc) {
        super(taskMemoryManager);
        this.spillThresholdSize = spillThresholdSize;
        this.spillFunc = spillFunc;
    }

    @Override
    public void spill(SizeTrackingAppendOnlyMap map) {
        Iterator<Tuple2<K, V>> iter = map.iterator();
        while (iter.hasNext()) {
            Tuple2<K, V> kv = iter.next();
            spillFunc.apply(kv);
        }
    }

    @Override
    public boolean forceSpill() {
        return false;
    }

    public void changeValue(K key, Function2 updateFunc) {
        long estimatedSize = currentMap.estimateSize();
        if (estimatedSize > spillThresholdSize) {
            spill(currentMap);
            currentMap = new SizeTrackingAppendOnlyMap<>();
            estimatedSize = currentMap.estimateSize();
        }
        if (maybeSpill(currentMap, estimatedSize)) {
            currentMap = new SizeTrackingAppendOnlyMap<>();
        }
        currentMap.changeValue(key, updateFunc);
        addElementsRead();
    }

    /**
     * It will return the iterator of the non-spilled elements in {@code currentMap}.
     */
    public Iterator<Tuple2<K, V>> iterator() {
        return currentMap.iterator();
    }

    public boolean isEmpty() {
        return currentMap.isEmpty();
    }
}
