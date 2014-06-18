package org.infinispan.hadoopintegration.mapreduce.input;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.*;
import org.infinispan.client.hotrod.impl.consistenthash.ConsistentHash;
import org.infinispan.hadoopintegration.InfinispanCache;
import org.infinispan.hadoopintegration.configuration.InfinispanConfiguration;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.*;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 7.0
 */
public class InfinispanInputFormat<K, V> implements InputFormat<K, V>, Configurable {

    private static Log log = LogFactory.getLog(InfinispanInputFormat.class);

    private InfinispanConfiguration configuration;

    public static final List<Integer> ALL_SEGMENTS = Collections.singletonList(-1);

    @Override
    public InputSplit[] getSplits(JobConf jobContext, int chunks) throws IOException {
        InfinispanCache<K, V> infinispanCache = InfinispanCache.getInputCacheForInputSplit(configuration);
        ConsistentHash consistentHash = infinispanCache.getRemoteCache().getConsistentHash();
        if (consistentHash == null) {
            return new InputSplit[]{createSingleServerInputSplit()};
        }
        Map<SegmentOwners, List<Integer>> ownersToSegmentsMap = new HashMap<SegmentOwners, List<Integer>>();
        List<InputSplit> inputSplitList = new LinkedList<InputSplit>();
        int segmentId = 0;
        for (SocketAddress[] owners : consistentHash.getSegmentOwners()) {
            SegmentOwners segmentOwners = new SegmentOwners(owners);
            //inputSplitList.add(new SegmentInputSplit(segmentOwners, Collections.singletonList(segmentId++)));
            List<Integer> segments = ownersToSegmentsMap.get(segmentOwners);
            if (segments == null) {
                segments = new LinkedList<Integer>();
                ownersToSegmentsMap.put(segmentOwners, segments);
            }
            segments.add(segmentId++);
        }

        for (Map.Entry<SegmentOwners, List<Integer>> entry : ownersToSegmentsMap.entrySet()) {
            inputSplitList.add(new SegmentInputSplit(entry.getKey(), entry.getValue()));
        }
        infinispanCache.stop();

        log.info("Created input list: " + inputSplitList);

        return inputSplitList.toArray(new InputSplit[inputSplitList.size()]);
    }

    @Override
    public RecordReader<K, V> getRecordReader(InputSplit inputSplit, JobConf entries, Reporter reporter) throws IOException {
        InfinispanInputConverter<K, V, Object, Object>  converter;
        try {
            converter = configuration.getInputConverter();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        InfinispanCache<Object, Object> infinispanCache = InfinispanCache.getInputCache(configuration);
        log.info("Creating record reader for input " + inputSplit + " and using " + infinispanCache);
        return new SegmentRecordReader<K, V, Object, Object>(inputSplit, infinispanCache, converter);
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
