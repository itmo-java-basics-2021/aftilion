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

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
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

        if (context.currentSegmentContext() == null) {
            throw new DatabaseException("Context segment is null");
        }

        int size = 0;
        SegmentInitializationContext segmentinitialContext = context.currentSegmentContext();
        SegmentIndex segIndex = segmentinitialContext.getIndex();
        Path segPath = segmentinitialContext.getSegmentPath();
        Segment segment = SegmentImpl.initializeFromContext(segmentinitialContext);
        List<String> keys = new ArrayList<>();

        if (!Files.exists(segPath)) {
            throw new DatabaseException(segmentinitialContext.getSegmentName() + " does not exist");
        }
        try (DatabaseInputStream InputStream = new DatabaseInputStream(new FileInputStream(segPath.toFile()))) {
            Optional<DatabaseRecord> dbUnitOp = InputStream.readDbUnit();
            while (dbUnitOp.isPresent()) {
                DatabaseRecord dbUnits = dbUnitOp.get();
                segIndex.onIndexedEntityUpdated(new String(dbUnits.getKey()), new SegmentOffsetInfoImpl(size));
                size += dbUnits.size();
                keys.add(new String(dbUnits.getKey()));
                dbUnitOp = InputStream.readDbUnit();
            }
        } catch (IOException ex) {
            throw new DatabaseException("Error while initialisation segment", ex);
        }

        Segment newSegment = SegmentImpl.initializeFromContext(new SegmentInitializationContextImpl(segmentinitialContext.getSegmentName(), segmentinitialContext.getSegmentPath(), size, segIndex));

        for (String in : keys) {
            context.currentTableContext().getTableIndex().onIndexedEntityUpdated(in, segment);
        }
        context.currentTableContext().updateCurrentSegment(newSegment);
    }
}


