package com.itmo.java.basics.logic.impl;

import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.index.SegmentOffsetInfo;
import com.itmo.java.basics.index.impl.SegmentIndex;
import com.itmo.java.basics.index.impl.SegmentOffsetInfoImpl;
import com.itmo.java.basics.logic.DatabaseRecord;
import com.itmo.java.basics.logic.Segment;
import com.itmo.java.basics.logic.io.DatabaseInputStream;
import com.itmo.java.basics.logic.io.DatabaseOutputStream;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;

/**
 * Сегмент - append-only файл, хранящий пары ключ-значение, разделенные специальным символом.
 * - имеет ограниченный размер, большие значения (>100000) записываются в последний сегмент, если он не read-only
 * - при превышении размера сегмента создается новый сегмент и дальнейшие операции записи производятся в него
 * - именование файла-сегмента должно позволять установить очередность их появления
 * - является неизменяемым после появления более нового сегмента
 */
public class SegmentImpl implements Segment {

    public Path tableRootPath;
    public String segmentName;
    public SegmentIndex segmentIndex;
    public final long sizeMaximum = 100000;
    public long segmentSize;
    private final DatabaseOutputStream outStream;

    public SegmentImpl(Path tableRootPath, String segmentName, OutputStream outStream) {
        this.tableRootPath = tableRootPath;
        this.segmentName = segmentName;
        this.segmentIndex = new SegmentIndex();
        this.outStream = new DatabaseOutputStream(outStream);
    }

    static Segment create(String segmentName, Path tableRootPath) throws DatabaseException {

        Path segRoot = Paths.get(tableRootPath.toString(), segmentName);
        boolean fileExists;
        OutputStream outputStream;

        try {
            fileExists = segRoot.toFile().createNewFile();
            outputStream = Files.newOutputStream(segRoot);

        } catch (IOException ex) {
            throw new DatabaseException("Creating Error " + segmentName + ex);
        }
        if (!fileExists) {
            throw new DatabaseException("Creating Error" + segmentName + "as it already exists");
        }

        return new SegmentImpl(segRoot, segmentName, outputStream);
    }

    static String createSegmentName(String tableName) {
        return tableName + "_" + System.currentTimeMillis();
    }

    @Override
    public String getName() {
        return segmentName;
    }

    @Override
    public boolean write(String objectKey, byte[] objectValue) throws IOException {

        if (isReadOnly()) {
            outStream.close();
            return false;
        }

        if (objectValue == null) {
            return delete(objectKey);
        }

        SetDatabaseRecord newSeg = new SetDatabaseRecord(objectKey.getBytes(StandardCharsets.UTF_8), objectValue);

        segmentIndex.onIndexedEntityUpdated(objectKey, new SegmentOffsetInfoImpl(segmentSize));
        segmentSize += outStream.write(newSeg);
        return true;
    }

    @Override
    public Optional<byte[]> read(String objectKey) throws IOException {

        Optional<SegmentOffsetInfo> offsetInfo = segmentIndex.searchForKey(objectKey);

        if (offsetInfo.isEmpty())
            return Optional.empty();
        long myOf = offsetInfo.get().getOffset();
        DatabaseInputStream input = new DatabaseInputStream(Files.newInputStream(tableRootPath));

        long skipped =  input.skip(myOf);

        if (skipped != myOf){
            throw new IOException("Error while skipping bytes " + segmentName);
        }
       // input.skip(myOf);


        Optional<DatabaseRecord> value = input.readDbUnit();

        if (value.isEmpty())
            return Optional.empty();

        input.close();
        return Optional.of(value.get().getValue());
    }

    @Override
    public boolean isReadOnly() { return segmentSize >= sizeMaximum;}

    @Override
    public boolean delete(String objectKey) throws IOException {

        if (isReadOnly()) {
            outStream.close();
            return false;
        }

        if (segmentIndex.searchForKey(objectKey).isEmpty()){
            outStream.close();
            return false;
        }


        RemoveDatabaseRecord newSeg = new RemoveDatabaseRecord(objectKey.getBytes());
        segmentIndex.onIndexedEntityUpdated(objectKey, new SegmentOffsetInfoImpl(segmentSize));
        segmentSize += outStream.write(newSeg);
        return true;
    }
}