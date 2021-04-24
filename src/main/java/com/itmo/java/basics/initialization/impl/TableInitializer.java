package com.itmo.java.basics.initialization.impl;

import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.index.impl.SegmentIndex;
import com.itmo.java.basics.initialization.InitializationContext;
import com.itmo.java.basics.initialization.Initializer;
import com.itmo.java.basics.logic.impl.TableImpl;

import java.io.File;
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
     *  или если возникла ошибка ошибка дочерних инициализаторов
     */
    @Override

    public void perform(InitializationContext context) throws DatabaseException {
        if (context.currentTableContext() == null) {
            throw new DatabaseException("Error with ContextTable"+ context.currentTableContext());
        }
        File directory = context.currentTableContext().getTablePath().toFile();
        if (directory.listFiles() == null) {
            return;
        }
        File[] segments = directory.listFiles();
        for (File seg : segments) {
            InitializationContext init = InitializationContextImpl.builder()
                    .executionEnvironment(context.executionEnvironment())
                    .currentDatabaseContext(context.currentDbContext())
                    .currentTableContext(context.currentTableContext())
                    .currentSegmentContext(new SegmentInitializationContextImpl(
                            seg.getName(), seg.toPath(), (int)seg.length(),
                            new SegmentIndex()))
                    .build();
            segmentInitializer.perform(init);
        }
        context.currentDbContext().addTable(TableImpl.initializeFromContext(context.currentTableContext()));
    }
}
