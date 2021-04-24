package com.itmo.java.basics.logic.impl;

import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.logic.DatabaseCache;
import com.itmo.java.basics.logic.Table;

import java.util.Optional;

public class CachingTable implements Table {

    private Table myTable;
    private DatabaseCache myDataBaseCache;

    static final int MAX_CACHE_CAPACITY = 10000;

    public CachingTable(Table myTable) {
        this.myTable = myTable;
        this.myDataBaseCache = new DatabaseCacheImpl(MAX_CACHE_CAPACITY);
    }

    @Override
    public String getName() {
        return myTable.getName();
    }

    @Override
    public void write(String objectKey, byte[] objectValue) throws DatabaseException {
        myTable.write(objectKey, objectValue);
        myDataBaseCache.set(objectKey, objectValue);
    }

    @Override
    public Optional<byte[]> read(String objectKey) throws DatabaseException {
        byte[] tryReadValueFromCache = myDataBaseCache.get(objectKey);
        if (tryReadValueFromCache != null) {
            return Optional.of(tryReadValueFromCache);
        } else {
            return myTable.read(objectKey);
        }
    }

    @Override
    public void delete(String objectKey) throws DatabaseException {
        myTable.delete(objectKey);
        myDataBaseCache.delete(objectKey);
    }
}