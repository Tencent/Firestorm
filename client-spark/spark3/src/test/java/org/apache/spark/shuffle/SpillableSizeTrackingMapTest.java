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

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.SparkEnv;
import org.apache.spark.memory.MemoryConsumer;
import org.apache.spark.memory.TaskMemoryManager;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import scala.Function1;
import scala.Function2;
import scala.Tuple2;
import scala.runtime.AbstractFunction1;
import scala.runtime.AbstractFunction2;

/**
 * The test cases for {@link SpillableSizeTrackingMap}
 */
public class SpillableSizeTrackingMapTest {
    private static SparkContext sparkContext;

    @BeforeAll
    public static void init() {
        SparkConf conf = new SparkConf(false);
        conf.setAppName("testApp").setMaster("local[2]");
        final SparkContext sc = SparkContext.getOrCreate(conf);
        sparkContext = sc;
    }

    @AfterAll
    public static void tearDown() {
        if (sparkContext != null) {
            sparkContext.stop();
        }
    }

    private void createMapAndCheck(long initialRequiredMemoryBytes, long totalMemoryBytes,
            long spillThresholdBytes) {
        TaskMemoryManager mockTaskMemoryManager = new MockedTaskMemoryManager(totalMemoryBytes);

        List<String> spillStoreList = new ArrayList<>();
        Function1<Tuple2<String, String>, Void> spillFunc1 = new AbstractFunction1<Tuple2<String, String>, Void>() {

            @Override
            public Void apply(Tuple2<String, String> kv) {
                spillStoreList.add(kv._1);
                return null;
            }
        };

        SpillableSizeTrackingMap<String, String> map = new SpillableSizeTrackingMap<>(
                mockTaskMemoryManager,
                spillThresholdBytes,
                spillFunc1,
                initialRequiredMemoryBytes
        );

        for (int i = 0; i < 1000; i++) {
            map.changeValue("key" + i, getUpdateFunc("val" + i));
        }
        Assertions.assertTrue(spillStoreList.size() > 0);
        Assertions.assertTrue(((MockedTaskMemoryManager)mockTaskMemoryManager).getUsed() > 0);

        map.finalizeAndClear();
        Assertions.assertEquals(1000, spillStoreList.size());
        Assertions.assertEquals(((MockedTaskMemoryManager)mockTaskMemoryManager).getUsed(), 0);
    }

    @Test
    public void testMapReachedSpillThreshold() {
        // Initial required: 1KB
        long initialRequiredMemoryBytes = 1024L;
        long totalMemoryBytes = Long.MAX_VALUE;
        // Spill threshold size: 2MB
        long spillThresholdBytes = 2 * 1024 * 1024L;
        createMapAndCheck(initialRequiredMemoryBytes, totalMemoryBytes, spillThresholdBytes);
    }

    @Test
    public void testMapAcquireMemoryFailedToRelaseMemory() {
        // Initial required: 1KB
        long initialRequiredMemoryBytes = 1024L;
        // Total memory: 1M
        long totalMemoryBytes = 1 * 1024 * 1024L;
        long spillThresholdBytes = Long.MAX_VALUE;
        createMapAndCheck(initialRequiredMemoryBytes, totalMemoryBytes, spillThresholdBytes);
    }

    @Test
    public void testWithoutReachingSpillCondition() {
        // Initial required: 1KB
        long initialRequiredMemoryBytes = 1024L;
        long totalMemoryBytes = Long.MAX_VALUE;
        long spillThresholdBytes = Long.MAX_VALUE;

        TaskMemoryManager mockTaskMemoryManager = new MockedTaskMemoryManager(totalMemoryBytes);

        List<String> spillStoreList = new ArrayList<>();
        Function1<Tuple2<String, String>, Void> spillFunc1 = new AbstractFunction1<Tuple2<String, String>, Void>() {

            @Override
            public Void apply(Tuple2<String, String> kv) {
                spillStoreList.add(kv._1);
                return null;
            }
        };

        SpillableSizeTrackingMap<String, String> map = new SpillableSizeTrackingMap<>(
                mockTaskMemoryManager,
                spillThresholdBytes,
                spillFunc1,
                initialRequiredMemoryBytes
        );

        for (int i = 0; i < 1000; i++) {
            map.changeValue("key" + i, getUpdateFunc("val" + i));
        }
        Assertions.assertTrue(spillStoreList.isEmpty());

        map.finalizeAndClear();
        Assertions.assertEquals(spillStoreList.size(), 1000);
    }

    private Function2 getUpdateFunc(String newVal) {
        Function2 updateFunc = new AbstractFunction2<Boolean, String, String>() {
            @Override
            public String apply(Boolean hasKey, String oldVal) {
                if (hasKey) {
                    return oldVal + ":" + newVal;
                } else {
                    return newVal;
                }
            }
        };
        return updateFunc;
    }

    class MockedTaskMemoryManager extends TaskMemoryManager {

        private final long totalMemoryBytes;
        private long used = 0L;

        /**
         * Construct a new MockedTaskMemoryManager.
         */
        public MockedTaskMemoryManager(long totalMemoryBytes) {
            super(SparkEnv.get().memoryManager(), 0);
            this.totalMemoryBytes = totalMemoryBytes;
        }

        @Override
        public long acquireExecutionMemory(long required, MemoryConsumer consumer) {
            long rest = totalMemoryBytes - used;
            if (rest > required) {
                used += required;
                return required;
            }
            used = totalMemoryBytes;
            return rest;
        }

        @Override
        public void releaseExecutionMemory(long size, MemoryConsumer consumer) {
            used -= size;
        }

        public long getUsed() {
            return used;
        }
    }
}
