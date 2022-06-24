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

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

import org.apache.spark.memory.MemoryConsumer;
import org.apache.spark.memory.TaskMemoryManager;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;

import scala.Function1;
import scala.Function2;
import scala.Tuple2;

/**
 * This class is to limit the memory usage from the task memory manager.
 * Once the memory usage exceed the {@code spillThresholdSize} or lack
 * the enough memory from task memory manager, it will spill elements by
 * the external function of {@code spillFunc}.
 */
public class SpillableSizeTrackingMap<K, V> extends AbstractSizeTracker {
    private final long DEFAULT_INITIAL_REQUIRED_MEMORY_BYTES = 5 * 1024 * 1024;
    private final long spillThresholdSize;
    private final Function1<Tuple2<K, V>, Void> spillFunc;

    private Map<K, V> currentMap = Maps.newHashMap();
    private long grantedMemorySize = 0;
    private MemoryConsumerImpl memoryConsumer;
    private long initialRequireMemorySize = DEFAULT_INITIAL_REQUIRED_MEMORY_BYTES;

    private class MemoryConsumerImpl extends MemoryConsumer {
        protected MemoryConsumerImpl(TaskMemoryManager taskMemoryManager) {
            super(taskMemoryManager);
        }

        @Override
        public long spill(long size, MemoryConsumer trigger) throws IOException {
            return 0L;
        }
    }

    public SpillableSizeTrackingMap(TaskMemoryManager taskMemoryManager, Long spillThresholdSize,
            Function1<Tuple2<K, V>, Void> spillFunc) {
        this.spillThresholdSize = spillThresholdSize;
        this.spillFunc = spillFunc;
        this.memoryConsumer = new MemoryConsumerImpl(taskMemoryManager);
    }

    @VisibleForTesting
    SpillableSizeTrackingMap(TaskMemoryManager taskMemoryManager, long spillThresholdSize,
            Function1<Tuple2<K, V>, Void> spillFunc, long initialRequireMemorySize) {
        this(taskMemoryManager, spillThresholdSize, spillFunc);
        this.initialRequireMemorySize = initialRequireMemorySize;
    }

    private void spill(Map currentMap) {
        for (Iterator<Map.Entry<K, V>> it = currentMap.entrySet().iterator(); it.hasNext();) {
            Map.Entry<K, V> entry = it.next();
            spillFunc.apply(Tuple2.apply(entry.getKey(), entry.getValue()));
            it.remove();
        }
    }

    public void changeValue(K key, Function2 updateFunc) {
        if (grantedMemorySize == 0) {
            grantedMemorySize += memoryConsumer.acquireMemory(initialRequireMemorySize);
        }

        long currentMemory = super.estimateSize();
        boolean shouldSpill = false;

        if (currentMemory > spillThresholdSize) {
            shouldSpill = true;
        } else if (currentMemory > grantedMemorySize) {
            long amountToRequest = 2 * currentMemory - grantedMemorySize;
            long granted = memoryConsumer.acquireMemory(amountToRequest);
            grantedMemorySize += granted;
            shouldSpill = currentMemory >= grantedMemorySize;
        }

        if (shouldSpill) {
            spill(currentMap);
            currentMap = Maps.newHashMap();
            super.resetSamples();
            if (grantedMemorySize > initialRequireMemorySize) {
                memoryConsumer.freeMemory(grantedMemorySize - initialRequireMemorySize);
                grantedMemorySize = initialRequireMemorySize;
            }
        }
        updateVal(key, updateFunc);
        super.afterUpdate();
    }

    private void updateVal(K key, Function2 updateFunc) {
        currentMap.computeIfAbsent(key, k -> (V) updateFunc.apply(false, null));
        currentMap.computeIfPresent(key, (k, oldVal) -> (V) updateFunc.apply(true, oldVal));
    }

    public boolean isEmpty() {
        return currentMap.isEmpty();
    }

    public void finalizeAndClear() {
        spill(currentMap);
        currentMap = Maps.newHashMap();
        memoryConsumer.freeMemory(grantedMemorySize);
        grantedMemorySize = 0;
        super.resetSamples();
    }
}
