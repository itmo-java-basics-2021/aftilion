package com.itmo.java.basics.initialization.impl;

import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.index.impl.TableIndex;
import com.itmo.java.basics.initialization.DatabaseInitializationContext;
import com.itmo.java.basics.initialization.InitializationContext;
import com.itmo.java.basics.initialization.Initializer;
import com.itmo.java.basics.logic.Database;
import com.itmo.java.basics.logic.impl.DatabaseImpl;

import java.io.File;
import java.nio.file.Files;

public class DatabaseInitializer implements Initializer {

    private final TableInitializer tableInitializer;

    public DatabaseInitializer(TableInitializer tableInitializer) {
        this.tableInitializer = tableInitializer;
    }

    /**
     * Добавляет в контекст информацию об инициализируемой бд.
     * Запускает инициализацию всех таблиц это базы
     *
     * @param context контекст с информацией об инициализируемой бд и об окружении
     * @throws DatabaseException если в контексте лежит неправильный путь к базе, невозможно прочитать содержимого папки,
     *                           или если возникла ошибка дочерних инициализаторов
     */
    @Override
    public void perform(InitializationContext context) throws DatabaseException {

        DatabaseInitializationContext dbinitialContext = context.currentDbContext();
        if (!Files.exists(dbinitialContext.getDatabasePath())) {
            throw new DatabaseException("We dont have this " + dbinitialContext.getDbName());
        }

        File curFile = new File(dbinitialContext.getDatabasePath().toString());

        if (!curFile.exists()) {
            throw new DatabaseException(dbinitialContext.getDbName() + " does not exist");
        }

        File[] directory = curFile.listFiles();

        if (directory == null) {
            throw new DatabaseException("Error while working with " + curFile.toString());
        }
        for (File table : directory) {
            TableInitializationContextImpl tableContext = new TableInitializationContextImpl(table.getName(), dbinitialContext.getDatabasePath(), new TableIndex());
            tableInitializer.perform(new InitializationContextImpl(context.executionEnvironment(), dbinitialContext, tableContext, null));
        }
        Database database = DatabaseImpl.initializeFromContext(dbinitialContext);
        context.executionEnvironment().addDatabase(database);
    }
}
