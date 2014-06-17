package org.infinispan.hadoopintegration.mapreduce.input;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.*;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.impl.consistenthash.ConsistentHash;
import org.infinispan.hadoopintegration.InfinispanObject;
import org.infinispan.hadoopintegration.configuration.InfinispanConfiguration;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 7.0
 */
public class InfinispanInputFormat<K, V> implements InputFormat<InfinispanObject<K>, InfinispanObject<V>>, Configurable {

    private InfinispanConfiguration configuration;

    public static final List<Integer> ALL_SEGMENTS = Collections.singletonList(-1);

    @Override
    public InputSplit[] getSplits(JobConf jobContext, int chunks) throws IOException {
        RemoteCacheManager remoteCacheManager = new RemoteCacheManager(configuration.getInputRemoteCacheHost(),
                configuration.getInputRemoteCachePort());
        RemoteCache<K, V> remoteCache = remoteCacheManager.getCache(configuration.getInputCacheName());
        ConsistentHash consistentHash = remoteCache.getConsistentHash();
        if (consistentHash == null) {
            return new InputSplit[]{createSingleServerInputSplit()};
        }
        //Map<SegmentOwners, List<Integer>> ownersToSegmentsMap = new HashMap<SegmentOwners, List<Integer>>();
        List<InputSplit> inputSplitList = new LinkedList<InputSplit>();
        int segmentId = 0;
        for (SocketAddress[] owners : consistentHash.getSegmentOwners()) {
            SegmentOwners segmentOwners = new SegmentOwners(owners);
            inputSplitList.add(new SegmentInputSplit(segmentOwners, Collections.singletonList(segmentId++)));
            /*List<Integer> segments = ownersToSegmentsMap.get(segmentOwners);
            if (segments == null) {
                segments = new LinkedList<Integer>();
                ownersToSegmentsMap.put(segmentOwners, segments);
            }
            segments.add(segmentId++);*/
        }

        /*int segmentSplits = ownersToSegmentsMap.size() / numMapReduceTasks;
        for (Map.Entry<SegmentOwners, List<Integer>> entry : ownersToSegmentsMap.entrySet()) {
            List<Integer> segments = entry.getValue();
            int partitionSize = segments.size() / segmentSplits;
            for (int i = 0; i < segments.size(); i += partitionSize) {
                inputSplitList.add(new SegmentInputSplit(entry.getKey(),
                        segments.subList(i, i + Math.min(partitionSize, segments.size() - i))));
            }
        }*/
        remoteCache.stop();
        remoteCacheManager.stop();

        return inputSplitList.toArray(new InputSplit[inputSplitList.size()]);
    }

    @Override
    public RecordReader<InfinispanObject<K>, InfinispanObject<V>> getRecordReader(InputSplit inputSplit, JobConf entries, Reporter reporter) throws IOException {
        RemoteCacheManager remoteCacheManager = new RemoteCacheManager(configuration.getInputRemoteCacheHost(),
                configuration.getInputRemoteCachePort());
        RemoteCache<K, V> remoteCache = remoteCacheManager.getCache(configuration.getInputCacheName());
        return new SegmentRecordReader<K, V>(inputSplit, remoteCacheManager, remoteCache);
    }


    @Override
    public void setConf(Configuration entries) {
        this.configuration = new InfinispanConfiguration(entries);
    }

    @Override
    public Configuration getConf() {
        return configuration.getConfiguration();
    }

    private InputSplit createSingleServerInputSplit() {
        SocketAddress[] addresses = new SocketAddress[1];
        addresses[0] = InetSocketAddress.createUnresolved(configuration.getInputRemoteCacheHost(),
                configuration.getInputRemoteCachePort());
        return new SegmentInputSplit(new SegmentOwners(addresses), ALL_SEGMENTS);
    }
}
