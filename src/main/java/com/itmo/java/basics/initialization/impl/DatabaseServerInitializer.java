package com.itmo.java.basics.initialization.impl;

import com.itmo.java.basics.console.ExecutionEnvironment;
import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.initialization.InitializationContext;
import com.itmo.java.basics.initialization.Initializer;

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

//        if (context.executionEnvironment() == null) {
//            throw new DatabaseException("Context executionEnvironment is null");
//        }

        ExecutionEnvironment ExecutionEnvironment = context.executionEnvironment();
        Path path = ExecutionEnvironment.getWorkingPath();
        File workingDir = new File(String.valueOf(path));

        if (!workingDir.exists()) {
            if (!workingDir.mkdir()) {
                throw new DatabaseException("While creating dir DVSIit");
            }
        }
        File[] curFiles = workingDir.listFiles(File::isDirectory);
        for (File dir : curFiles) {
            DatabaseInitializationContextImpl dbContext = new DatabaseInitializationContextImpl(dir.getName(),path);
            InitializationContextImpl newContext = InitializationContextImpl.builder()
                    .currentDatabaseContext(dbContext)
                    .executionEnvironment(context.executionEnvironment()).build();
            databaseInitializer.perform(newContext);
        }
    }
}






