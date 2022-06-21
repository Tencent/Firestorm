package org.apache.spark.shuffle;

import org.apache.spark.memory.TaskMemoryManager;
import org.apache.spark.util.collection.SizeTrackingAppendOnlyMap;
import org.apache.spark.util.collection.Spillable;

import scala.Function1;
import scala.Function2;
import scala.Tuple2;
import scala.collection.Iterator;

/**
 * @author zhangjunfan
 * @date 2022/6/21
 */
public class MemoryLimitedMap<K, V> extends Spillable<SizeTrackingAppendOnlyMap> {
    private SizeTrackingAppendOnlyMap<K, V> currentMap = new SizeTrackingAppendOnlyMap<>();
    private final long maxSpillThresholdSize;
    private final Function1<Tuple2<K, V>, Void> spillFunc;

    public MemoryLimitedMap(TaskMemoryManager taskMemoryManager, Long maxSpillThresholdSize,
            Function1<Tuple2<K, V>, Void> spillFunc) {
        super(taskMemoryManager);
        this.maxSpillThresholdSize = maxSpillThresholdSize;
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
        if (estimatedSize > maxSpillThresholdSize) {
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

    public Iterator<Tuple2<K, V>> iterator() {
        return currentMap.iterator();
    }

    public boolean isEmpty() {
        return currentMap.isEmpty();
    }
}
