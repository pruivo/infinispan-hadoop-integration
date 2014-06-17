package org.infinispan.hadoopintegration.mapreduce.input;

import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.RecordReader;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.impl.consistenthash.ConsistentHash;
import org.infinispan.hadoopintegration.InfinispanObject;

import java.io.IOException;
import java.util.*;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 7.0
 */
public class SegmentRecordReader<K, V> implements RecordReader<InfinispanObject<K>, InfinispanObject<V>> {

    private List<K> keys;
    private final RemoteCacheManager remoteCacheManager;
    private final RemoteCache<K, V> remoteCache;
    private int keyIndex;
    private K currentKey;
    private V currentValue;

    public SegmentRecordReader(InputSplit inputSplit, RemoteCacheManager remoteCacheManager,
                               RemoteCache<K, V> remoteCache) {
        this.remoteCacheManager = remoteCacheManager;
        this.remoteCache = remoteCache;
        this.keyIndex = -1;
        initialize(inputSplit);
    }

    @Override
    public boolean next(InfinispanObject<K> kInfinispanObject, InfinispanObject<V> vInfinispanObject) throws IOException {
        if (!nextKeyValue()) {
            return false;
        }
        kInfinispanObject.set(currentKey);
        vInfinispanObject.set(currentValue);
        return true;
    }

    @Override
    public InfinispanObject<K> createKey() {
        return new InfinispanObject<K>();
    }

    @Override
    public InfinispanObject<V> createValue() {
        return new InfinispanObject<V>();
    }

    @Override
    public long getPos() throws IOException {
        return keyIndex;
    }

    @Override
    public void close() throws IOException {
        remoteCache.stop();
        remoteCacheManager.stop();
    }

    private void initialize(InputSplit inputSplit) {
        List<Integer> segmentIds = ((SegmentInputSplit) inputSplit).getSegmentsId();
        if (segmentIds.equals(InfinispanInputFormat.ALL_SEGMENTS)) {
            keys = new ArrayList<K>(remoteCache.keySet());
        } else {
            ConsistentHash consistentHash = remoteCache.getConsistentHash();
            Set<Integer> segmentsIds = new HashSet<Integer>(segmentIds);
            keys = new LinkedList<K>();

            for (K key : remoteCache.keySet()) {
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
            currentValue = remoteCache.get(currentKey);
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
