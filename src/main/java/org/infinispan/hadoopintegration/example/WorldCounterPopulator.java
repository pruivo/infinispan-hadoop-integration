package org.infinispan.hadoopintegration.example;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.impl.ConfigurationProperties;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.*;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 7.0
 */
public class WorldCounterPopulator {

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

        String filePath = map.get(Argument.FILE);
        if (filePath == null) {
            System.err.println(Argument.FILE.arg + " is missing!");
            System.exit(1);
        }

        File file = new File(filePath);
        if (!file.exists()) {
            System.err.println("File '" + filePath + "' not found!");
            System.exit(1);
        }
        BufferedReader reader = new BufferedReader(new FileReader(file));

        RemoteCacheManager remoteCacheManager = new RemoteCacheManager(map.get(Argument.HOST),
                Integer.parseInt(map.get(Argument.PORT)));
        RemoteCache<Integer, String> remoteCache = remoteCacheManager.getCache(map.get(Argument.CACHE_NAME));

        String line;
        int lineNumber = 1;
        while ((line = reader.readLine()) != null) {
            remoteCache.put(lineNumber++, line);
        }

        reader.close();
        remoteCache.stop();
        remoteCacheManager.stop();
        System.exit(0);
    }

    public static enum Argument {
        HOST("--host", true, "localhost"),
        PORT("--port", true, Integer.toString(ConfigurationProperties.DEFAULT_HOTROD_PORT)),
        CACHE_NAME("--cachename", true, RemoteCacheManager.DEFAULT_CACHE_NAME),
        FILE("--file", true, null),
        HELP("--help", false, null);

        private final String arg;
        private final boolean hasValue;
        private final String defaultValue;

        Argument(String arg, boolean hasValue, String defaultValue) {
            this.arg = arg;
            this.hasValue = hasValue;
            this.defaultValue = defaultValue;
        }

        public static void setDefaultValues(Map<Argument, String> map) {
            for (Argument argument : values()) {
                if (argument.defaultValue != null) {
                    map.put(argument, argument.defaultValue);
                }
            }
        }

        public static void parse(Queue<String> args, Map<Argument, String> map) {
            String arg = args.poll();
            if (arg == null) {
                return;
            }
            for (Argument argument : values()) {
                if (arg.equals(argument.arg)) {
                    if (argument.hasValue) {
                        String value = args.poll();
                        if (value == null) {
                            throw new IllegalArgumentException("Argument " + arg + " expects a value!");
                        }
                        map.put(argument, value);
                    } else {
                        map.put(argument, Boolean.toString(true));
                    }
                    return;
                }
            }
        }

        public static String help(Argument argument) {
            StringBuilder builder = new StringBuilder();
            builder.append(argument.arg);
            builder.append(" ");
            if (argument.hasValue) {
                builder.append("<value> ");
            }
            if (argument.defaultValue != null) {
                builder.append("(default=");
                builder.append(argument.defaultValue);
                builder.append(")");
            }
            return builder.toString();
        }
    }

}
