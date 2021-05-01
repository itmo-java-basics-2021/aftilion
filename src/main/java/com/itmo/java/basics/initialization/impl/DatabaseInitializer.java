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
        if (context.currentDbContext() == null) {
            throw new DatabaseException("Context Db is null");
        }
        File dir = context.currentDbContext().getDatabasePath().toFile();

        if (dir.listFiles() == null) {
            return;
        }

        File[] tables = dir.listFiles();
        for (File table : tables) {
            InitializationContext init = new InitializationContextImpl(context.executionEnvironment(),
                    context.currentDbContext(),
                    new TableInitializationContextImpl(table.getName(), table.toPath(), new TableIndex()),
                    context.currentSegmentContext());

            tableInitializer.perform(init);
        }

        context.executionEnvironment().addDatabase(DatabaseImpl.initializeFromContext(context.currentDbContext()));
    }
}
