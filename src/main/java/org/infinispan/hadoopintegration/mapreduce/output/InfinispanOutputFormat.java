package org.infinispan.hadoopintegration.mapreduce.output;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.Progressable;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.hadoopintegration.configuration.InfinispanConfiguration;

import java.io.IOException;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 7.0
 */
public class InfinispanOutputFormat<K, V> implements OutputFormat<K, V>, Configurable {

    private InfinispanConfiguration configuration;

    @Override
    public RecordWriter<K, V> getRecordWriter(FileSystem fileSystem, JobConf entries, String s, Progressable progressable) throws IOException {
        RemoteCacheManager remoteCacheManager = new RemoteCacheManager(configuration.getOutputRemoteCacheHost(),
                configuration.getOutputRemoteCachePort());
        RemoteCache<K, V> remoteCache = remoteCacheManager.getCache(configuration.getOutputCacheName());
        return new InfinispanRecordWriter<K, V>(remoteCacheManager, remoteCache);
    }

    @Override
    public void checkOutputSpecs(FileSystem fileSystem, JobConf entries) throws IOException {
        //nothing to validate for now.
    }

    @Override
    public void setConf(Configuration entries) {
        this.configuration = new InfinispanConfiguration(entries);
    }

    @Override
    public Configuration getConf() {
        return configuration.getConfiguration();
    }
}
