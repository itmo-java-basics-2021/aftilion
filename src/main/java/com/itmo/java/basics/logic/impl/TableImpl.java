package com.itmo.java.basics.logic.impl;

import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.index.impl.TableIndex;
import com.itmo.java.basics.logic.Segment;
import com.itmo.java.basics.logic.Table;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;

public class TableImpl implements Table {

    public String tableName;
    public Path pathToDatabaseRoot;
    public TableIndex tableIndex;
    public Segment lastSegment;

    private TableImpl(String tableName, Path pathToDatabaseRoot, TableIndex tableIndex) throws DatabaseException {
        this.tableName = tableName;
        this.pathToDatabaseRoot = pathToDatabaseRoot;
        this.tableIndex = tableIndex;
        this.lastSegment = SegmentImpl.create(SegmentImpl.createSegmentName(tableName), pathToDatabaseRoot);
    }

    static Table create(String tableName, Path pathToDatabaseRoot, TableIndex tableIndex) throws DatabaseException {

        if (tableName == null)
            throw new DatabaseException("Why tableBase name is null?");

        Path pathToTableRoot = Paths.get(pathToDatabaseRoot.toString(), tableName);

        try {
            Files.createDirectory(pathToTableRoot);
        } catch (IOException ex) {
            throw new DatabaseException("Error while creating a Table(" + tableName + ") directory");
        }

        return new TableImpl(tableName, pathToTableRoot, tableIndex);
    }

    @Override
    public String getName() { return tableName; }

    @Override
    public void write(String objectKey, byte[] objectValue) throws DatabaseException {

        try {
            boolean canWewrite = lastSegment.write(objectKey, objectValue);

            if (!canWewrite) {
                lastSegment = SegmentImpl.create(SegmentImpl.createSegmentName(tableName), pathToDatabaseRoot);
                lastSegment.write(objectKey, objectValue);
            }
            tableIndex.onIndexedEntityUpdated(objectKey, lastSegment);
        } catch (IOException ex) {
            throw new DatabaseException("Writing in database error " + ex);
        }
    }

    @Override
    public Optional<byte[]> read(String objectKey) throws DatabaseException {

        try {
            Optional<Segment> segmentRead = tableIndex.searchForKey(objectKey);

            if (segmentRead.isPresent()) {
                return segmentRead.get().read(objectKey);

            } else
                return Optional.empty();
        } catch (IOException ex) {
            throw new DatabaseException("Reading in database error " + ex);
        }
    }

    @Override
    public void delete(String objectKey) throws DatabaseException {

        try {
            boolean canDel = lastSegment.delete(objectKey);

            if (!canDel) {
                this.lastSegment = SegmentImpl.create(SegmentImpl.createSegmentName(tableName), pathToDatabaseRoot);
                this.lastSegment.delete(objectKey);
            }

            tableIndex.onIndexedEntityUpdated(objectKey, null);
        } catch (IOException ex) {
            throw new DatabaseException("Deleting error in Table");
        }
    }
}
