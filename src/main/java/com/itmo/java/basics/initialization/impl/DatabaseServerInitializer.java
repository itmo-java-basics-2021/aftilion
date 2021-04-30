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

        ExecutionEnvironment ExecutionEnvironment = context.executionEnvironment();
        Path path = ExecutionEnvironment.getWorkingPath();

        if(!Files.exists(path)){
            try{
                Files.createDirectory(path);
            }catch (IOException e){
                throw new DatabaseException("Problems occurred while creating directory " + path.toString(), e);
            }
        }
        File curFile = new File(path.toString());
        File[] files = curFile.listFiles();
        if(files == null){
            throw new DatabaseException("Error while working with" + curFile.toString());
        }
        for (File in : files) {
            DatabaseInitializationContextImpl dbContext = new DatabaseInitializationContextImpl(in.getName(), path);
            databaseInitializer.perform(new InitializationContextImpl(context.executionEnvironment(), dbContext, null, null));
        }
    }
}






