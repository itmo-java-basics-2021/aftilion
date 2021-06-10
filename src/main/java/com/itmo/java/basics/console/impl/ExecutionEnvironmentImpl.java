package com.itmo.java.basics.console.impl;

import com.itmo.java.basics.config.DatabaseConfig;
import com.itmo.java.basics.console.ExecutionEnvironment;
import com.itmo.java.basics.logic.Database;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class ExecutionEnvironmentImpl implements ExecutionEnvironment {

    private final DatabaseConfig dbConfig;
    private final Map<String, Database> dataBase;


    public ExecutionEnvironmentImpl(DatabaseConfig config) {
        dbConfig = config;
        dataBase = new HashMap<>();
    }

    @Override
    public Optional<Database> getDatabase(String name) {
        return Optional.ofNullable(dataBase.get(name));
    }

    @Override
    public void addDatabase(Database db) {
        dataBase.put(db.getName(), db);
    }

    @Override
    public Path getWorkingPath() {
        return Path.of(dbConfig.getWorkingPath());
    }
}