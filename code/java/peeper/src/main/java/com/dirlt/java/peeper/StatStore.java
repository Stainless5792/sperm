package com.dirlt.java.peeper;

import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: dirlt
 * Date: 12/8/12
 * Time: 1:42 AM
 * To change this template use File | Settings | File Templates.
 */

public class StatStore {
    // singleton.
    private Map<String, Long> counter = new TreeMap<String, Long>();
    private Configuration configuration;
    private static final int kReservedSize = 10;
    private static final int kTickInterval = 60 * 1000;
    private static Configuration gConfiguration;
    private static StatStore[] pool = new StatStore[kReservedSize];
    private static volatile int current = 0;

    public static StatStore getInstance() {
        return pool[current];
    }

    public static StatStore getInstance(int index) {
        return pool[index];
    }

    public static String getStat() {
        if (gConfiguration.isStat()) {
            StringBuffer sb = new StringBuffer();
            sb.append(String.format("Service : %s\n", gConfiguration.getServiceName()));
            sb.append(String.format("=====configuration=====\n%s\n", gConfiguration.toString()));
            for (int i = 0; i < kReservedSize; i++) {
                int index = (current - i + kReservedSize) % kReservedSize;
                sb.append(String.format("=====last %d minutes=====\n", i));
                getInstance(index).getStat(sb);
                sb.append("\n");
            }
            return sb.toString();
        } else {
            return "statistics off";
        }
    }

    public StatStore(Configuration configuration) {
        this.configuration = configuration;
    }

    public static void init(Configuration configuration) {
        gConfiguration = configuration;
        for (int i = 0; i < kReservedSize; i++) {
            pool[i] = new StatStore(configuration);
        }
        Timer tickTimer = new Timer(true);
        tickTimer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                int next = (current + 1) % kReservedSize;
                pool[next].counter.clear();
                current = next;
            }
        }, 0, kTickInterval);
    }

    public void addCounter(String name, long value) {
        if (!configuration.isStat()) {
            return;
        }
        synchronized (counter) {
            if (counter.containsKey(name)) {
                counter.put(name, counter.get(name) + value);
            } else {
                counter.put(name, value);
            }
        }
    }

    // well a little too simple.:).
    private void getStat(StringBuffer sb) {
        synchronized (counter) {
            Set<Map.Entry<String, Long>> entries = counter.entrySet();
            for (Map.Entry<String, Long> entry : entries) {
                sb.append(String.format("%s = %s\n", entry.getKey(), entry.getValue().toString()));
            }
        }
    }
}