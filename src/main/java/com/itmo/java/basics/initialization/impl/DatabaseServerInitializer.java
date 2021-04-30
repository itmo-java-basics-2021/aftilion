package com.itmo.java.basics.initialization.impl;

import com.itmo.java.basics.console.ExecutionEnvironment;
import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.index.impl.TableIndex;
import com.itmo.java.basics.initialization.InitializationContext;
import com.itmo.java.basics.initialization.Initializer;
import com.itmo.java.basics.logic.impl.DatabaseImpl;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class DatabaseServerInitializer implements Initializer {
    private final Initializer databaseInitializer;

    public DatabaseServerInitializer(DatabaseInitializer databaseInitializer) {
        this.databaseInitializer = databaseInitializer;
    }

    /**
     * Если заданная в окружении директория не существует - создает ее
     * Добавляет информацию о существующих в директории базах, нацинает их инициалиализацию
     *
     * @param context контекст, содержащий информацию об окружении
     * @throws DatabaseException если произошла ошибка при создании директории, ее обходе или ошибка инициализации бд
     */
    @Override
    public void perform(InitializationContext context) throws DatabaseException {

        if (context.executionEnvironment() == null) {
            throw new DatabaseException("Context Env is null");
        }

        if (!Files.exists(context.executionEnvironment().getWorkingPath())) {
            try {
                Files.createDirectory(context.executionEnvironment().getWorkingPath());
            } catch (Exception ex) {
                throw new DatabaseException("Error while creating directory");
            }
        }

        File dir = context.executionEnvironment().getWorkingPath().toFile();

        if (dir.listFiles() == null) {
            return;
        }

        File[] files = dir.listFiles();
        for (File file : files) {
            if (file.isDirectory()) {
                InitializationContext init = InitializationContextImpl.builder()
                        .executionEnvironment(context.executionEnvironment())
                        .currentDatabaseContext(new DatabaseInitializationContextImpl(file.getName(), context.executionEnvironment().getWorkingPath())).build();


                databaseInitializer.perform(init);
            }
        }
    }
}






