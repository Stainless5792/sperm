package com.dirlt.java.FastHBaseRest;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import java.util.concurrent.TimeUnit;

/**
 * Created with IntelliJ IDEA.
 * User: dirlt
 * Date: 12/8/12
 * Time: 2:40 AM
 * To change this template use File | Settings | File Templates.
 */
public class LocalCache {
    // singleton.
    private static Cache<String, byte[]> cache = null;
    private static LocalCache instance = null;

    public static void init(Configuration configuration) {
        instance = new LocalCache(configuration);
    }

    private LocalCache(Configuration configuration) {
        CacheBuilder builder = CacheBuilder.newBuilder();
        builder.expireAfterWrite(configuration.getCacheExpireTime(), TimeUnit.SECONDS);
        builder.maximumSize(configuration.getCacheMaxCapacity());
        cache = builder.build();
    }

    public static LocalCache getInstance() {
        return instance;
    }

    public byte[] get(String k) {
        return cache.getIfPresent(k);
    }

    public void set(String k, byte[] b) {
        cache.put(k, b);
    }

    public void clear() {
        cache.invalidateAll();
    }
}
