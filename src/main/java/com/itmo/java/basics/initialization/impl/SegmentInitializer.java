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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
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

        SegmentInitializationContext segmentinitialContext = context.currentSegmentContext();
        SegmentIndex segmentIndex = segmentinitialContext.getIndex();
        Path segmentPath = segmentinitialContext.getSegmentPath();
        Segment segment = SegmentImpl.initializeFromContext(segmentinitialContext);
        int offset = 0;

        if (!Files.exists(segmentPath)) {
            throw new DatabaseException(segmentinitialContext.getSegmentName() + " does not exist");
        }
        ArrayList<String> keys = new ArrayList<>();
        try (DatabaseInputStream InputStream = new DatabaseInputStream(new FileInputStream(segmentPath.toFile()))) {
            Optional<DatabaseRecord> dbUnitOptional = InputStream.readDbUnit();
            while (dbUnitOptional.isPresent()) {
                DatabaseRecord dbUnit = dbUnitOptional.get();
                segmentIndex.onIndexedEntityUpdated(new String(dbUnit.getKey()), new SegmentOffsetInfoImpl(offset));
                offset += dbUnit.size();
                keys.add(new String(dbUnit.getKey()));
                dbUnitOptional = InputStream.readDbUnit();
            }
        } catch (IOException ex) {
            throw new DatabaseException("Error while initialisation segment", ex);
        }
        Segment newSegment = SegmentImpl.initializeFromContext(new SegmentInitializationContextImpl(segmentinitialContext.getSegmentName(), segmentinitialContext.getSegmentPath(), offset, segmentIndex));
        for (String i : keys){
            context.currentTableContext().getTableIndex().onIndexedEntityUpdated(i, segment);
        }
        context.currentTableContext().updateCurrentSegment(newSegment);
    }
}


