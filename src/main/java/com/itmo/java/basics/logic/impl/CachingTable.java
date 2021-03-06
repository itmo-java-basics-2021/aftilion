package com.itmo.java.basics.logic.impl;

import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.logic.DatabaseCache;
import com.itmo.java.basics.logic.Table;

import java.util.Optional;

public class CachingTable implements Table {

    private final Table cacheTable;
    private final DatabaseCache dbCache;

    public CachingTable(Table myTable) {
        this.cacheTable = myTable;
        this.dbCache = new DatabaseCacheImpl();
    }

    @Override
    public String getName() {
        return cacheTable.getName();
    }

    @Override
    public void write(String objectKey, byte[] objectValue) throws DatabaseException {
        cacheTable.write(objectKey, objectValue);
        dbCache.set(objectKey, objectValue);
    }

    @Override
    public Optional<byte[]> read(String objectKey) throws DatabaseException {
        byte[] reading = dbCache.get(objectKey);
        if (reading != null) {
            return Optional.of(reading);
        } else {
            return cacheTable.read(objectKey);
        }
    }

    @Override
    public void delete(String objectKey) throws DatabaseException {
        cacheTable.delete(objectKey);
        dbCache.delete(objectKey);
    }
}