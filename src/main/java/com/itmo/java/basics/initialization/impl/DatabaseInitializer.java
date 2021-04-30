package com.itmo.java.basics.initialization.impl;

import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.index.impl.TableIndex;
import com.itmo.java.basics.initialization.DatabaseInitializationContext;
import com.itmo.java.basics.initialization.InitializationContext;
import com.itmo.java.basics.initialization.Initializer;
import com.itmo.java.basics.logic.impl.DatabaseImpl;

import java.io.File;
import java.nio.file.Files;

public class DatabaseInitializer implements Initializer {
    private final Initializer tableInitializer;
    public DatabaseInitializer(TableInitializer tableInitializer) {
        this.tableInitializer = tableInitializer;
    }

    /**
     * Добавляет в контекст информацию об инициализируемой бд.
     * Запускает инициализацию всех таблиц это базы
     *
     * @param initialContext контекст с информацией об инициализируемой бд и об окружении
     * @throws DatabaseException если в контексте лежит неправильный путь к базе, невозможно прочитать содержимого папки,
     *  или если возникла ошибка дочерних инициализаторов
     */
    @Override
    public void perform(InitializationContext initialContext) throws DatabaseException {

        DatabaseInitializationContext dbInitializationContext = initialContext.currentDbContext();
        if(!Files.exists(dbInitializationContext.getDatabasePath())){
            throw new DatabaseException("We dont have this DataBase" + dbInitializationContext.getDbName());
        }

        if (initialContext.currentDbContext() == null) {
            throw new DatabaseException("Error with ContextTable"+ initialContext.currentTableContext());
        }


        File directory = initialContext.currentDbContext().getDatabasePath().toFile();
        if (directory.listFiles() == null) {
            return;
        }
        File[] tables = directory.listFiles();
        for (File table : tables) {
            InitializationContext init = new InitializationContextImpl(initialContext.executionEnvironment(),
                    initialContext.currentDbContext(),
                    new TableInitializationContextImpl(table.getName(), table.toPath(), new TableIndex()),
                    initialContext.currentSegmentContext());
            tableInitializer.perform(init);
        }
        initialContext.executionEnvironment().addDatabase(DatabaseImpl.initializeFromContext(initialContext.currentDbContext()));
    }
}
