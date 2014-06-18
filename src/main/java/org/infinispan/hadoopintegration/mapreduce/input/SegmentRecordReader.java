package org.infinispan.hadoopintegration.mapreduce.input;

import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.RecordReader;
import org.infinispan.client.hotrod.impl.consistenthash.ConsistentHash;
import org.infinispan.hadoopintegration.InfinispanCache;

import java.io.IOException;
import java.util.*;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 7.0
 */
public class SegmentRecordReader<K, V, K1, V1> implements RecordReader<K, V> {

    private List<K1> keys;
    private final InfinispanCache<K1, V1> infinispanCache;
    private final InfinispanInputConverter<K, V, K1, V1> converter;
    private int keyIndex;
    private K1 currentKey;
    private V1 currentValue;

    public SegmentRecordReader(InputSplit inputSplit, InfinispanCache<K1, V1> infinispanCache, InfinispanInputConverter<K, V, K1, V1> converter) {
        this.infinispanCache = infinispanCache;
        this.converter = converter;
        this.keyIndex = -1;
        initialize(inputSplit);
    }

    @Override
    public boolean next(K kInfinispanObject, V vInfinispanObject) throws IOException {
        if (!nextKeyValue()) {
            return false;
        }
        converter.setKey(kInfinispanObject, currentKey);
        converter.setValue(vInfinispanObject, currentValue);
        return true;
    }

    @Override
    public K createKey() {
        return converter.createKey();
    }

    @Override
    public V createValue() {
        return converter.createValue();
    }

    @Override
    public long getPos() throws IOException {
        return keyIndex;
    }

    @Override
    public void close() throws IOException {
        infinispanCache.stop();
    }

    private void initialize(InputSplit inputSplit) {
        List<Integer> segmentIds = ((SegmentInputSplit) inputSplit).getSegmentsId();
        if (segmentIds.equals(InfinispanInputFormat.ALL_SEGMENTS)) {
            keys = new ArrayList<K1>(infinispanCache.getRemoteCache().keySet());
        } else {
            ConsistentHash consistentHash = infinispanCache.getRemoteCache().getConsistentHash();
            Set<Integer> segmentsIds = new HashSet<Integer>(segmentIds);
            keys = new LinkedList<K1>();

            for (K1 key : infinispanCache.getRemoteCache().keySet()) {
                if (segmentsIds.contains(segmentOf(key, consistentHash))) {
                    keys.add(key);
                }
            }
        }
    }

    private static int segmentOf(Object key, ConsistentHash consistentHash) {
        return consistentHash.getNormalizedHash(key) / consistentHash.getSegmentOwners().length;
    }

    public boolean nextKeyValue() {
        currentKey = null;
        currentValue = null;
        while (++keyIndex < keys.size()) {
            currentKey = keys.get(keyIndex);
            currentValue = infinispanCache.getRemoteCache().get(currentKey);
            if (currentValue != null) {
                return true;
            }
        }
        currentKey = null;
        return false;
    }

    @Override
    public float getProgress() throws IOException {
        return keys.size() == 0 ? 1 : keyIndex / keys.size();
    }
}
