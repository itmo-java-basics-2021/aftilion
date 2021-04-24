package com.itmo.java.basics.logic.impl;
import java.util.LinkedHashMap;
import java.util.Map;
import com.itmo.java.basics.logic.DatabaseCache;

public class DatabaseCacheImpl implements DatabaseCache {

    private static final int defineSize = 100_000;
    private final LinkedHashMap<String, String> cache;

    public DatabaseCacheImpl() {
        cache = new LinkedHashMap<>(defineSize, 1f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<String, String> eldest) {
                return size() > defineSize;
            }
        };
    }

    @Override
    public byte[] get(String key) {
        return new byte[0];
    }

    @Override
    public void set(String key, byte[] value) {

    }

    @Override
    public void delete(String key) {

    }
}
