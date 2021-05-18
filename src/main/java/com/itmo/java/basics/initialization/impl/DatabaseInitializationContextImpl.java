package com.itmo.java.basics.initialization.impl;

import com.itmo.java.basics.initialization.DatabaseInitializationContext;
import com.itmo.java.basics.logic.Table;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

public class DatabaseInitializationContextImpl implements DatabaseInitializationContext {

    private final String dbName;
    private final Path dbRoot;
    private  Map<String, Table> tablesMap = new HashMap<>();

    public DatabaseInitializationContextImpl(String dbName, Path dbRoot) {
        this.dbName = dbName;
        this.dbRoot = Paths.get(dbRoot.toString(),dbName);
    }

    @Override
    public String getDbName() {
        return dbName;
    }

    @Override
    public Path getDatabasePath() {
        return Paths.get(dbRoot.toString(),dbName);
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
