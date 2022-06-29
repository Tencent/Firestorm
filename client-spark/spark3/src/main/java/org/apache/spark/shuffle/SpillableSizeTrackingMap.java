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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import org.apache.spark.memory.MemoryConsumer;
import org.apache.spark.memory.MemoryMode;
import org.apache.spark.memory.TaskMemoryManager;
import org.apache.spark.util.collection.SizeTracker;
import scala.Function1;
import scala.Function2;
import scala.Tuple2;
import scala.collection.mutable.Queue;

/**
 * This class is to limit the memory usage from the task memory manager.
 * Once the memory usage exceed the {@code spillThresholdSize} or lack
 * the enough memory from task memory manager, it will spill elements by
 * the external function of {@code spillFunc}.
 */
public class SpillableSizeTrackingMap<K, V> extends MemoryConsumer implements SizeTracker {
    private static final long DEFAULT_INITIAL_REQUIRED_MEMORY_BYTES = 5 * 1024 * 1024;
    private final long spillThresholdSize;
    private final Function1<Tuple2<K, V>, Void> spillFunc;

    private Map<K, V> currentMap = Maps.newHashMap();
    private long grantedMemorySize = 0;
    private long initialRequireMemorySize = DEFAULT_INITIAL_REQUIRED_MEMORY_BYTES;

    private double sampleGrowthRate = 1.1;
    private long nextSampleNum;
    private long numUpdates;
    private double bytesPerUpdate;
    private Queue samples = new Queue<SizeTracker.Sample>();

    public SpillableSizeTrackingMap(TaskMemoryManager taskMemoryManager, Long spillThresholdSize,
            Function1<Tuple2<K, V>, Void> spillFunc) {
        super(taskMemoryManager, taskMemoryManager.pageSizeBytes(), MemoryMode.ON_HEAP);
        this.spillThresholdSize = spillThresholdSize;
        this.spillFunc = spillFunc;
        resetSamples();
    }

    @VisibleForTesting
    SpillableSizeTrackingMap(TaskMemoryManager taskMemoryManager, long spillThresholdSize,
            Function1<Tuple2<K, V>, Void> spillFunc, long initialRequireMemorySize) {
        this(taskMemoryManager, spillThresholdSize, spillFunc);
        this.initialRequireMemorySize = initialRequireMemorySize;
        resetSamples();
    }

    private void doSpill(Map currentMap) {
        for (Iterator<Map.Entry<K, V>> it = currentMap.entrySet().iterator(); it.hasNext();) {
            Map.Entry<K, V> entry = it.next();
            it.remove();
            spillFunc.apply(Tuple2.apply(entry.getKey(), entry.getValue()));
        }
    }

    public void changeValue(K key, Function2 updateFunc) {
        if (grantedMemorySize == 0) {
            grantedMemorySize += acquireMemory(initialRequireMemorySize);
        }

        long currentMemory = estimateSize();
        boolean shouldSpill = false;

        if (currentMemory > spillThresholdSize) {
            shouldSpill = true;
        } else if (currentMemory > grantedMemorySize) {
            long amountToRequest = 2 * currentMemory - grantedMemorySize;
            long granted = acquireMemory(amountToRequest);
            grantedMemorySize += granted;
            shouldSpill = currentMemory >= grantedMemorySize;
        }

        if (shouldSpill) {
            doSpill(currentMap);
            currentMap = Maps.newHashMap();
            resetSamples();
            if (grantedMemorySize > initialRequireMemorySize) {
                freeMemory(grantedMemorySize - initialRequireMemorySize);
                grantedMemorySize = initialRequireMemorySize;
            }
        }
        updateVal(key, updateFunc);
        afterUpdate();
    }

    private void updateVal(K key, Function2 updateFunc) {
        currentMap.computeIfAbsent(key, k -> (V) updateFunc.apply(false, null));
        currentMap.computeIfPresent(key, (k, oldVal) -> (V) updateFunc.apply(true, oldVal));
    }

    public boolean isEmpty() {
        return currentMap.isEmpty();
    }

    public void finalizeAndClear() {
        doSpill(currentMap);
        currentMap = Maps.newHashMap();
        freeMemory(grantedMemorySize);
        grantedMemorySize = 0;
        resetSamples();
    }

    @Override
    public long spill(long size, MemoryConsumer trigger) throws IOException {
        return 0L;
    }

    /**
     * The following code is to implement methods of {@link SizeTracker}.
      */

    // CHECKSTYLE:OFF

    @SuppressWarnings("checkstyle:LineLength")
    public void org$apache$spark$util$collection$SizeTracker$_setter_$org$apache$spark$util$collection$SizeTracker$$SAMPLE_GROWTH_RATE_$eq(final double sampleGrowthRate) {
        this.sampleGrowthRate = sampleGrowthRate;
    }

    public double org$apache$spark$util$collection$SizeTracker$$SAMPLE_GROWTH_RATE() {
        return this.sampleGrowthRate;
    }

    @SuppressWarnings("checkstyle:LineLength")
    public void org$apache$spark$util$collection$SizeTracker$_setter_$org$apache$spark$util$collection$SizeTracker$$samples_$eq(final Queue samples) {
        this.samples = samples;
    }

    public Queue org$apache$spark$util$collection$SizeTracker$$samples() {
        return this.samples;
    }

    public double org$apache$spark$util$collection$SizeTracker$$bytesPerUpdate() {
        return this.bytesPerUpdate;
    }

    public void org$apache$spark$util$collection$SizeTracker$$bytesPerUpdate_$eq(final double bytesPerUpdate) {
        this.bytesPerUpdate = bytesPerUpdate;
    }

    public long org$apache$spark$util$collection$SizeTracker$$numUpdates() {
        return this.numUpdates;
    }

    public void org$apache$spark$util$collection$SizeTracker$$numUpdates_$eq(long numUpdates) {
        this.numUpdates = numUpdates;
    }

    public long org$apache$spark$util$collection$SizeTracker$$nextSampleNum() {
        return this.nextSampleNum;
    }

    public void org$apache$spark$util$collection$SizeTracker$$nextSampleNum_$eq(long nextSampleNum) {
        this.nextSampleNum = nextSampleNum;
    }

    // CHECKSTYLE:ON
}
