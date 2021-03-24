package com.itmo.java.basics.logic.impl;

import com.itmo.java.basics.index.SegmentOffsetInfo;
import com.itmo.java.basics.index.impl.SegmentIndex;
import com.itmo.java.basics.index.impl.SegmentOffsetInfoImpl;
import com.itmo.java.basics.logic.DatabaseRecord;
import com.itmo.java.basics.logic.Segment;
import com.itmo.java.basics.exceptions.DatabaseException;
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

    public  Path tableRootPath;
    public String segmentName;
    public SegmentIndex segmentIndex;
    public final long size_M = 100000;
    public long segmentSize ;
    private final DatabaseOutputStream out;

    public SegmentImpl(Path tableRootPath, String segmentName, OutputStream out) {

        this.tableRootPath = tableRootPath;
        this.segmentName = segmentName;
        this.segmentIndex = new SegmentIndex();
        this.out = new DatabaseOutputStream(out);

    }

    static Segment create(String segmentName, Path tableRootPath) throws DatabaseException {

        Path segRoot = Paths.get(tableRootPath.toString(), segmentName);
        boolean have;
        OutputStream outputStream;

        try
        {
            have = segRoot.toFile().createNewFile();
            outputStream = Files.newOutputStream(segRoot);

        }
        catch(IOException exception)
        {
            throw new DatabaseException("Creating Error " + segmentName);
        }
        if(!have)
        {
            throw new DatabaseException("Creating Error" + segmentName + "as it already exists");
        }

        return new SegmentImpl(segRoot, segmentName, outputStream);
    }

    static String createSegmentName(String tableName)
    {
        return tableName + "_" + System.currentTimeMillis();
    }

    @Override
    public String getName()
    {
        return segmentName;
    }

    @Override
    public boolean write(String objectKey, byte[] objectValue) throws IOException {

        if(isReadOnly())
        {
            out.close();
            return false;
        }

        if(objectValue == null)
        {
            return delete(objectKey);
        }

        SetDatabaseRecord newSeg= new SetDatabaseRecord(objectKey.getBytes(StandardCharsets.UTF_8), objectValue);

        segmentIndex.onIndexedEntityUpdated(objectKey, new SegmentOffsetInfoImpl(segmentSize));
        segmentSize += out.write(newSeg);
        return true;
    }

    @Override
    public Optional<byte[]> read(String objectKey) throws IOException {

        //if (segmentSize == 0)
         //   return Optional.empty();

        Optional<SegmentOffsetInfo> offsetInfo = segmentIndex.searchForKey(objectKey);

        if(offsetInfo.isEmpty())
            return Optional.empty();


        long myOf = offsetInfo.get().getOffset();

        DatabaseInputStream input = new DatabaseInputStream(Files.newInputStream(tableRootPath));
        input.skip(myOf);

        Optional<DatabaseRecord> value = input.readDbUnit();

        if(value.isEmpty())
            return Optional.empty();

        input.close();
            return Optional.of(value.get().getValue());
    }

    @Override
    public boolean isReadOnly() {
        if (segmentSize >= size_M)
        return true;
        else
            return  false;
    }

    @Override
    public boolean delete(String objectKey) throws IOException {


        if(isReadOnly())
        {
            out.close();
            return false;
        }

        RemoveDatabaseRecord newSeg = new RemoveDatabaseRecord(objectKey.getBytes());
        segmentIndex.onIndexedEntityUpdated(objectKey, new SegmentOffsetInfoImpl(segmentSize));
        segmentSize += out.write(newSeg);
        return true;
    }
}
