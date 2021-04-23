package com.itmo.java.basics.logic.impl;

import com.itmo.java.basics.initialization.SegmentInitializationContext;
import com.itmo.java.basics.logic.Segment;
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

public class SegmentImpl implements Segment {



    private Path tableRootPath;
    private String segmentName;
    private SegmentIndex segmentIndex;
    private final long sizeMaximum = 100000;
    private long segmentSize;
    private final DatabaseOutputStream outStream;

    public SegmentImpl(Path tableRootPath, String segmentName, OutputStream outStream) {
        this.tableRootPath = tableRootPath;
        this.segmentName = segmentName;
        this.segmentIndex = new SegmentIndex();
        this.outStream = new DatabaseOutputStream(outStream);
    }

    public static Segment create(String segmentName, Path tableRootPath) throws DatabaseException {
        Path segmentRoot = Paths.get(tableRootPath.toString(), segmentName);
        boolean isCreated;
        OutputStream outputStream;

        try{
            isCreated = segmentRoot.toFile().createNewFile();
            outputStream = Files.newOutputStream(segmentRoot);
        }catch(IOException ex){
            throw new DatabaseException("Error while creating segment " + segmentName, ex);
        }
        if(!isCreated){
            throw new DatabaseException("Error while creating segment " + segmentName + "as it already exists");
        }
        return new SegmentImpl(segmentRoot, segmentName, outputStream);
    }

    public static Segment initializeFromContext(SegmentInitializationContext context) {
        return null;
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
        if (offsetInfo.isEmpty()) {
            return Optional.empty();
        }

        long myOf = offsetInfo.get().getOffset();
        try (DatabaseInputStream in = new DatabaseInputStream(Files.newInputStream(tableRootPath))) {
            long skipped = in.skip(myOf);
            if (skipped != myOf) {
                throw new IOException("Error while skipping bytes in segment called " + segmentName);
            }
            Optional<DatabaseRecord> value = in.readDbUnit();

            if (value.isEmpty()) {
                return Optional.empty();
            }

            return Optional.of(value.get().getValue());

        } catch (IOException exception) {
            throw new IOException("Error while creating a Segment file " + segmentName, exception);
        }
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