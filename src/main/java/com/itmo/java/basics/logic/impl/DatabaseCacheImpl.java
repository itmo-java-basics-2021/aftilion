package com.itmo.java.basics.logic.impl;

import com.itmo.java.basics.logic.DatabaseCache;

import java.util.LinkedHashMap;
import java.util.Map;

public class DatabaseCacheImpl implements DatabaseCache {

    private final int capacity;
    private final Map<String, byte[]> myCache;

    public DatabaseCacheImpl(int maxCapacity) {
        this.capacity = maxCapacity;
        this.myCache = new LinkedHashMap<String, byte[]>(capacity, 1f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<String, byte[]> eldest) {
                return size() > capacity;
            }
        };
    }

    @Override
    public byte[] get(String key) {
        return myCache.get(key);
    }

    @Override
    public void set(String key, byte[] value) {
        myCache.put(key, value);
    }

    @Override
    public void delete(String key) {
        myCache.remove(key);
    }
}