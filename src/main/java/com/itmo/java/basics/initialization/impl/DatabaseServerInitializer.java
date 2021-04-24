package com.itmo.java.basics.initialization.impl;

import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.index.impl.TableIndex;
import com.itmo.java.basics.initialization.InitializationContext;
import com.itmo.java.basics.initialization.Initializer;
import com.itmo.java.basics.logic.impl.DatabaseImpl;

import java.io.File;
import java.io.IOException;

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

            if (context.currentDbContext() == null) {
                throw new DatabaseException("Context Db is null");
            }
                File dir = context.currentDbContext().getDatabasePath().toFile();
                if (dir.listFiles() == null) {
                    return;
                }
                File[] tablesDataBase = dir.listFiles();
                for (File table : tablesDataBase) {
                    InitializationContext init = new InitializationContextImpl(context.executionEnvironment(),
                            context.currentDbContext(),
                            new TableInitializationContextImpl(table.getName(), table.toPath(), new TableIndex()),
                            context.currentSegmentContext());
                    databaseInitializer.perform(init);
                }
                context.executionEnvironment().addDatabase(DatabaseImpl.initializeFromContext(context.currentDbContext()));
            }
        }


