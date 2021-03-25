package com.itmo.java.basics.logic.impl;

import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.index.impl.TableIndex;
import com.itmo.java.basics.logic.Segment;
import com.itmo.java.basics.logic.Table;
import com.itmo.java.basics.logic.io.DatabaseOutputStream;

import javax.imageio.IIOException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class TableImpl implements Table {

    public  String tableName;
    public  Path pathToDatabaseRoot;
    public  TableIndex tableIndex;
    public  Segment lastSegment;



    private TableImpl(String tableName, Path pathToDatabaseRoot, TableIndex tableIndex) throws DatabaseException {
        this.tableName = tableName;
        this.pathToDatabaseRoot = pathToDatabaseRoot;
        this.tableIndex = tableIndex;
        this.lastSegment = SegmentImpl.create(SegmentImpl.createSegmentName(tableName),pathToDatabaseRoot);
    }

    static Table create(String tableName, Path pathToDatabaseRoot, TableIndex tableIndex) throws DatabaseException {

        if (tableName == null)
            throw new DatabaseException("Why tableBase name is null?");


        Path pathToTableRoot = Paths.get(pathToDatabaseRoot.toString() , tableName);

        try
        {
            Files.createDirectory(pathToTableRoot);
        }
        catch(IOException ex)
        {
            throw new DatabaseException("Error while creating a Table("+tableName+") directory");
        }

        return new TableImpl(tableName , pathToTableRoot , tableIndex);

    static Table create(String tableName, Path pathToDatabaseRoot, TableIndex tableIndex) throws DatabaseException {
/**
 * Таблица - логическая сущность, представляющая собой набор файлов-сегментов, которые объединены одним
 * именем и используются для хранения однотипных данных (данных, представляющих собой одну и ту же сущность,
 * например, таблица "Пользователи")
 * <p>
 * - имеет единый размер сегмента
 * - представляет из себя директорию в файловой системе, именованную как таблица
 * и хранящую файлы-сегменты данной таблицы
 */
public class TableImpl implements Table {
    public static Table create(String tableName, Path pathToDatabaseRoot, TableIndex tableIndex) throws DatabaseException {
        return null;
    }

    public static Table initializeFromContext(TableInitializationContext context) {
        return null;
    }

    @Override
    public String getName() {

        return tableName;
    }

    @Override
    public void write(String objectKey, byte[] objectValue) throws DatabaseException{

        try
        {
            boolean canWewrite = lastSegment.write(objectKey, objectValue);

            if (!canWewrite)
            {

                this.lastSegment = (SegmentImpl) SegmentImpl.create(SegmentImpl.createSegmentName(tableName),pathToDatabaseRoot);
                this.lastSegment.write(objectKey ,objectValue);
            }
            tableIndex.onIndexedEntityUpdated(objectKey,lastSegment);
        }
        catch(IOException ex)
        {
          throw new DatabaseException("Writing in database error " + ex);
        }


    }

    @Override
    public Optional<byte[]> read(String objectKey) throws DatabaseException {

        try
        {
            Optional<Segment> segmentRead = tableIndex.searchForKey(objectKey);

            if(segmentRead.isPresent())
            {
               return segmentRead.get().read(objectKey);

            }

            else
                return  Optional.empty();
        }

        catch (IOException ex) {
             throw new DatabaseException("Reading in database error " + ex);
        }
    }

    @Override
    public void delete(String objectKey) throws DatabaseException {

        try
        {
        boolean canDel = lastSegment.delete(objectKey);

        if(!canDel)
        {
            this.lastSegment = (SegmentImpl) SegmentImpl.create(SegmentImpl.createSegmentName(tableName),pathToDatabaseRoot);
            this.lastSegment.delete(objectKey);
        }

        tableIndex.onIndexedEntityUpdated(objectKey , null);
        }
        catch(IOException ex)
        {
            throw new DatabaseException("Deleting error in Table");
        }
    }
}
