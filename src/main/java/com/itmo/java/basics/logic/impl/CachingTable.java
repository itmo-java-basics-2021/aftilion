package com.itmo.java.basics.logic.impl;

import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.logic.DatabaseCache;
import com.itmo.java.basics.logic.Table;

import java.util.Optional;

public class CachingTable implements Table {

    private final Table table;
    private final DatabaseCache cache;

    public CachingTable(Table table) {
        this.table = table;
        this.cache = new DatabaseCacheImpl();
    }

    public CachingTable(Table table, DatabaseCache cache) {
        this.table = table;
        this.cache = cache;
    }
    @Override
    public String getName() {
      return table.getName();
    }

    @Override
    public void write(String objectKey, byte[] objectValue) throws DatabaseException {
     cache.set(objectKey ,objectValue);
     table.write(objectKey ,objectValue);

    }

    @Override
    public Optional<byte[]> read(String objectKey) throws DatabaseException {
        return Optional.empty();
    }

    @Override
    public void delete(String objectKey) throws DatabaseException {

    }
}
