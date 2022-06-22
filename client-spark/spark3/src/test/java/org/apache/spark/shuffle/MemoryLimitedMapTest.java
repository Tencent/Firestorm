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

import static org.mockito.Mockito.mock;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.SparkEnv;
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
 * The test case for {@link MemoryLimitedMap}
 */
public class MemoryLimitedMapTest {
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

    @Test
    public void testMapReachedSpillThreshold() {
        SparkEnv.get().conf().set("spark.shuffle.spill.initialMemoryThreshold", String.valueOf(32 * 1024 * 1024));
        TaskMemoryManager mockTaskMemoryManager = mock(TaskMemoryManager.class);

        List<String> spillStoreList = new ArrayList<>();
        Function1<Tuple2<String, String>, Void> spillFunc1 = new AbstractFunction1<Tuple2<String, String>, Void>() {

            @Override
            public Void apply(Tuple2<String, String> kv) {
                spillStoreList.add(kv._1);
                return null;
            }
        };

        MemoryLimitedMap<String, String> map = new MemoryLimitedMap<>(
                mockTaskMemoryManager,
                1024L,
                spillFunc1
        );

        for (int i = 0; i < 1000; i++) {
            map.changeValue("key" + i, getUpdateFunc("val" + i));
        }

        Assertions.assertTrue(spillStoreList.size() > 0);
    }

    @Test
    public void testMapAcquireMemoryFailed() {
        SparkEnv.get().conf().set("spark.shuffle.spill.initialMemoryThreshold", "1024");
        TaskMemoryManager mockTaskMemoryManager = mock(TaskMemoryManager.class);

        List<String> spillStoreList = new ArrayList<>();
        Function1<Tuple2<String, String>, Void> spillFunc1 = new AbstractFunction1<Tuple2<String, String>, Void>() {

            @Override
            public Void apply(Tuple2<String, String> kv) {
                spillStoreList.add(kv._1);
                return null;
            }
        };

        MemoryLimitedMap<String, String> map = new MemoryLimitedMap<>(
                mockTaskMemoryManager,
                Long.MAX_VALUE,
                spillFunc1
        );

        for (int i = 0; i < 1000; i++) {
            map.changeValue("key" + i, getUpdateFunc("val" + i));
        }

        Assertions.assertTrue(spillStoreList.size() > 0);
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
}
