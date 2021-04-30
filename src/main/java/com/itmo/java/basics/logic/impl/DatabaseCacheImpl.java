package com.itmo.java.basics.logic.impl;

import com.itmo.java.basics.logic.DatabaseCache;

import java.util.LinkedHashMap;
import java.util.Map;

public class DatabaseCacheImpl implements DatabaseCache {

    private final int dbCapacity ;
    private final Map<String, byte[]> dbCache;

    public DatabaseCacheImpl(int dbCapacity) {
        this.dbCapacity = dbCapacity;
        this.dbCache = new LinkedHashMap<String, byte[]>(dbCapacity, 1f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<String, byte[]> eldest) {
                return size() > dbCapacity;
            }
        };
    }

    @Override
    public byte[] get(String key) {
        return dbCache.get(key);
    }

    @Override
    public void set(String key, byte[] value) {
        dbCache.put(key, value);
    }

    @Override
    public void delete(String key) {
        dbCache.remove(key);
    }
}