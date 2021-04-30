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
    private final Initializer tableInitializer;
    public DatabaseInitializer(TableInitializer tableInitializer) {
        this.tableInitializer = tableInitializer;
    }

    /**
     * Добавляет в контекст информацию об инициализируемой бд.
     * Запускает инициализацию всех таблиц это базы
     *
     * @param context контекст с информацией об инициализируемой бд и об окружении
     * @throws DatabaseException если в контексте лежит неправильный путь к базе, невозможно прочитать содержимого папки,
     *  или если возникла ошибка дочерних инициализаторов
     */
    @Override
    public void perform(InitializationContext context) throws DatabaseException {

        DatabaseInitializationContext dbInitializationContext = context.currentDbContext();
        if(!Files.exists(dbInitializationContext.getDatabasePath())){
            throw new DatabaseException("We dont have this DataBase" + dbInitializationContext.getDbName());
        }
        File directory = new File(dbInitializationContext.getDatabasePath().toString());
        
        if(!directory.exists()){
            throw new DatabaseException(dbInitializationContext.getDbName() + "doesnt exists");
        }
        File[] tables = directory.listFiles();
        if(tables == null){
            throw new DatabaseException("Error with working" + directory.toString());
      }
        for (File table : tables) {
            TableInitializationContextImpl tableContext = new TableInitializationContextImpl(table.getName(), dbInitializationContext.getDatabasePath(), new TableIndex());
            tableInitializer.perform(new InitializationContextImpl(context.executionEnvironment(), dbInitializationContext, tableContext,null));
        }
        Database database = DatabaseImpl.initializeFromContext(dbInitializationContext);
        context.executionEnvironment().addDatabase(database);
    }
}
