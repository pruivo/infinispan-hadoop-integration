package org.infinispan.hadoopintegration.configuration;

import org.apache.hadoop.conf.Configuration;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.impl.ConfigurationProperties;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 7.0
 */
public class InfinispanConfiguration {

    private static final String INPUT_REMOTE_CACHE_HOST = "mapreduce.ispn.input.remote.cache.host";
    private static final String INPUT_REMOTE_CACHE_PORT = "mapreduce.ispn.input.remote.cache.port";
    private static final String INPUT_REMOTE_CACHE_NAME = "mapreduce.ispn.input.cache.name";

    private static final String OUTPUT_REMOTE_CACHE_HOST = "mapreduce.ispn.output.remote.cache.host";
    private static final String OUTPUT_REMOTE_CACHE_PORT = "mapreduce.ispn.output.remote.cache.port";
    private static final String OUTPUT_REMOTE_CACHE_NAME = "mapreduce.ispn.output.cache.name";

    private final Configuration configuration;

    public InfinispanConfiguration(Configuration configuration) {
        this.configuration = configuration;
    }

    public Configuration getConfiguration() {
        return configuration;
    }

    public String getInputRemoteCacheHost() {
        return configuration.get(INPUT_REMOTE_CACHE_HOST, "localhost");
    }

    public int getInputRemoteCachePort() {
        return configuration.getInt(INPUT_REMOTE_CACHE_PORT, ConfigurationProperties.DEFAULT_HOTROD_PORT);
    }

    public String getInputCacheName() {
        return configuration.get(INPUT_REMOTE_CACHE_NAME, RemoteCacheManager.DEFAULT_CACHE_NAME);
    }

    public String getOutputRemoteCacheHost() {
        return configuration.get(OUTPUT_REMOTE_CACHE_HOST, "localhost");
    }

    public int getOutputRemoteCachePort() {
        return configuration.getInt(OUTPUT_REMOTE_CACHE_PORT, ConfigurationProperties.DEFAULT_HOTROD_PORT);
    }

    public String getOutputCacheName() {
        return configuration.get(OUTPUT_REMOTE_CACHE_NAME, RemoteCacheManager.DEFAULT_CACHE_NAME);
    }


}
