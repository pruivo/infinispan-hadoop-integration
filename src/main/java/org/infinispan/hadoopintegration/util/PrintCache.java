package org.infinispan.hadoopintegration.util;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;

import java.net.SocketAddress;
import java.util.*;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 7.0
 */
public class PrintCache {

    public static void main(String[] args) throws Exception {
        Map<Argument, String> map = new EnumMap<Argument, String>(Argument.class);
        Queue<String> queue = new LinkedList<String>(Arrays.asList(args));
        Argument.setDefaultValues(map);
        while (!queue.isEmpty()) {
            Argument.parse(queue, map);
        }

        if (map.containsKey(Argument.HELP)) {
            System.out.println("The following arguments are allowed:");
            for (Argument argument : Argument.values()) {
                System.out.println(Argument.help(argument));
            }
            System.exit(0);
        }

        RemoteCacheManager remoteCacheManager = new RemoteCacheManager(map.get(Argument.HOST),
                Integer.parseInt(map.get(Argument.PORT)));
        RemoteCache<Object, Object> remoteCache = remoteCacheManager.getCache(map.get(Argument.CACHE_NAME));

        Set<SocketAddress> cluster = new HashSet<SocketAddress>();
        for (SocketAddress[] members : remoteCache.getConsistentHash().getSegmentOwners()) {
            cluster.addAll(Arrays.asList(members));
        }
        for (int i = 0; i< cluster.size(); ++i) {
            for (Map.Entry<Object, Object> entry : remoteCache.getBulk().entrySet()) {
                System.out.println(entry.getKey() + "\t" + entry.getValue());
            }
        }
        remoteCache.stop();
        remoteCacheManager.stop();
        System.exit(0);
    }
}
