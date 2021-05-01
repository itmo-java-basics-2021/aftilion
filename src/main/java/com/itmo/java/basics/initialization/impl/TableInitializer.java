package com.itmo.java.basics.initialization.impl;

import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.initialization.InitializationContext;
import com.itmo.java.basics.initialization.Initializer;
import com.itmo.java.basics.initialization.SegmentInitializationContext;
import com.itmo.java.basics.initialization.TableInitializationContext;
import com.itmo.java.basics.logic.Table;
import com.itmo.java.basics.logic.impl.TableImpl;

import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class TableInitializer implements Initializer {

    private final Initializer segmentInitializer;

    public TableInitializer(SegmentInitializer segmentInitializer) {
        this.segmentInitializer = segmentInitializer;
    }

    /**
     * Добавляет в контекст информацию об инициализируемой таблице.
     * Запускает инициализацию всех сегментов в порядке их создания (из имени)
     *
     * @param context контекст с информацией об инициализируемой бд, окружении, таблицы
     * @throws DatabaseException если в контексте лежит неправильный путь к таблице, невозможно прочитать содержимого папки,
     *                           или если возникла ошибка ошибка дочерних инициализаторов
     */
    @Override



    public void perform(InitializationContext context) throws DatabaseException {

        if (context.currentDbContext() == null) {
            throw new DatabaseException("Context Db is null");
        }

        TableInitializationContext tbinitalContext = context.currentTableContext();
        File curFile = new File(tbinitalContext.getTablePath().toString());

        if (!curFile.exists()) {
            throw new DatabaseException("Directory " + tbinitalContext.getTableName() + " does not exist");
        }

        File[] files = curFile.listFiles();

//        if (files == null) {
//            throw new DatabaseException("Error while working " + curFile.toString());
//        }

        List<File> segments = Arrays.asList(files);
        Collections.sort(segments);

        for (File seg : segments) {
            SegmentInitializationContext segmentContext = new SegmentInitializationContextImpl(seg.getName(), tbinitalContext.getTablePath(), 0);
            segmentInitializer.perform(new InitializationContextImpl(context.executionEnvironment(), context.currentDbContext(), context.currentTableContext(), segmentContext));
        }

        Table newTable = TableImpl.initializeFromContext(tbinitalContext);
        context.currentDbContext().addTable(newTable);
    }
}
