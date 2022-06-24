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

import org.apache.spark.util.SizeEstimator;

import scala.collection.JavaConverters;
import scala.collection.mutable.Queue;

/**
 * The class is the copy of {@link org.apache.spark.util.collection.SizeTracker},
 * just to work around the inability to implement the Scala trait of
 * {@link org.apache.spark.util.collection.SizeTracker} in Java directly.
 */
public abstract class AbstractSizeTracker {

    private double SAMPLE_GROWTH_RATE = 1.1;
    private Queue<Sample> samples = new Queue<>();
    private double bytesPerUpdate = 0;

    private long numUpdates = 0L;

    /** The value of 'numUpdates' at which we will take our next sample. */
    private long nextSampleNum = 0L;

    protected AbstractSizeTracker() {
        resetSamples();
    }

    protected void resetSamples() {
        numUpdates = 1;
        nextSampleNum = 1;
        samples.clear();
        takeSample();
    }

    /**
     * Callback to be invoked after every update.
     */
    protected void afterUpdate() {
        numUpdates += 1;
        if (nextSampleNum == numUpdates) {
            takeSample();
        }
    }

    /**
     * Take a new sample of the current collection's size.
     */
    private void takeSample() {
        List<Sample> list = new ArrayList<Sample>();
        list.add(new Sample(SizeEstimator.estimate(this), numUpdates));
        samples.enqueue(JavaConverters.asScalaBuffer(list));
        // Only use the last two samples to extrapolate
        if (samples.size() > 2) {
            samples.dequeue();
        }
        long bytesDelta = 0L;
        if (samples.size() == 2) {
            List<Sample> sampleList = JavaConverters.seqAsJavaList(samples.toList().reverse());
            bytesDelta =
                    (sampleList.get(0).size - sampleList.get(1).size) /
                            (sampleList.get(0).numUpdates - sampleList.get(1).numUpdates);
        }
        bytesPerUpdate = Math.max(0, bytesDelta);
        nextSampleNum = (long) Math.ceil(numUpdates * SAMPLE_GROWTH_RATE);
    }

    /**
     * Estimate the current size of the collection in bytes. O(1) time.
     */
    long estimateSize() {
        assert(samples.nonEmpty());
        long extrapolatedDelta = (long) (bytesPerUpdate * (numUpdates - samples.last().getNumUpdates()));
        return samples.last().getSize() + extrapolatedDelta;
    }

    class Sample {
        private long size;
        private long numUpdates;

        public Sample(long size, long numUpdates) {
            this.size = size;
            this.numUpdates = numUpdates;
        }

        public long getSize() {
            return size;
        }

        public long getNumUpdates() {
            return numUpdates;
        }
    }
}
