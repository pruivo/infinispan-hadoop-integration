package org.infinispan.hadoopintegration.mapreduce.output;

import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;

import java.io.IOException;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 7.0
 */
public class InfinispanRecordWriter<K, V> implements RecordWriter<K, V> {

    private final RemoteCache<K, V> remoteCache;
    private final RemoteCacheManager remoteCacheManager;

    public InfinispanRecordWriter(RemoteCacheManager remoteCacheManager, RemoteCache<K, V> remoteCache) {
        this.remoteCache = remoteCache;
        this.remoteCacheManager = remoteCacheManager;
    }

    @Override
    public void write(K k, V v) throws IOException {
        remoteCache.put(k, v);
    }

    @Override
    public void close(Reporter reporter) throws IOException {
        remoteCache.stop();
        remoteCacheManager.stop();
    }
}
