package com.itmo.java.basics.initialization.impl;

import com.itmo.java.basics.initialization.DatabaseInitializationContext;
import com.itmo.java.basics.logic.Table;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

public class DatabaseInitializationContextImpl implements DatabaseInitializationContext {

    private final String dbName;
    private final Path dbRoot;
    private final Map<String, Table> tablesMap;

    public DatabaseInitializationContextImpl(String dbName, Path databaseRoot) {
        this.dbName = dbName;
        this.dbRoot = databaseRoot;
        this.tablesMap = new HashMap<>();
    }

    @Override
    public String getDbName() {
        return dbName;
    }

    @Override
    public Path getDatabasePath() {
        return dbRoot;
    }

    @Override
    public Map<String, Table> getTables() {
        return tablesMap;
    }

    @Override
    public void addTable(Table table) {
        tablesMap.put(table.getName(), table);
    }
}
