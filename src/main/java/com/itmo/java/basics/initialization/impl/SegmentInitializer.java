package com.itmo.java.basics.initialization.impl;

import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.index.impl.SegmentIndex;
import com.itmo.java.basics.index.impl.SegmentOffsetInfoImpl;
import com.itmo.java.basics.initialization.InitializationContext;
import com.itmo.java.basics.initialization.Initializer;
import com.itmo.java.basics.initialization.SegmentInitializationContext;
import com.itmo.java.basics.logic.DatabaseRecord;
import com.itmo.java.basics.logic.Segment;
import com.itmo.java.basics.logic.impl.SegmentImpl;
import com.itmo.java.basics.logic.io.DatabaseInputStream;
import com.itmo.java.basics.logic.io.DatabaseOutputStream;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Optional;


public class SegmentInitializer implements Initializer {


    /**
     * Добавляет в контекст информацию об инициализируемом сегменте.
     * Составляет индекс сегмента
     * Обновляет инфу в индексе таблицы
     *
     * @param context контекст с информацией об инициализируемой бд и об окружении
     * @throws DatabaseException если в контексте лежит неправильный путь к сегменту, невозможно прочитать содержимое. Ошибка в содержании
     */
    @Override
    public void perform(InitializationContext context) throws DatabaseException {
        SegmentInitializationContext segmentInContext = context.currentSegmentContext();
        Path segmentPath = segmentInContext.getSegmentPath();
        Segment segmentIn =  SegmentImpl.initializeFromContext(segmentInContext);
        SegmentIndex segmentIndex =  segmentInContext.getIndex();
        int offSet = 0;
        try (DatabaseInputStream inputStream = new DatabaseInputStream( new FileInputStream(segmentPath.toString()))) {
            Optional<DatabaseRecord> dbUn = inputStream.readDbUnit();
            while(dbUn.isPresent()){
                DatabaseRecord databaseUnit = dbUn.get();
                if ( databaseUnit.isValuePresented()) {
                    segmentIndex.onIndexedEntityUpdated(new String(databaseUnit.getKey()), new SegmentOffsetInfoImpl(offSet));
                }
                else {
                    segmentIndex.onIndexedEntityUpdated(new String(databaseUnit.getKey()) , null);
                }
                context.currentTableContext().getTableIndex().onIndexedEntityUpdated(new String(databaseUnit.getKey()) , segmentIn);
                offSet += databaseUnit.size();
                dbUn = inputStream.readDbUnit();
                }
        }
        catch (IOException ex) {
            throw new DatabaseException("Error while InitializationContext " , ex);
        }
    }
}

