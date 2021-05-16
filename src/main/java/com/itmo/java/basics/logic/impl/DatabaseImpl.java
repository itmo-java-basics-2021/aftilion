package com.itmo.java.basics.logic.impl;

import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.index.impl.TableIndex;
import com.itmo.java.basics.logic.Database;
import com.itmo.java.basics.logic.Table;

import javax.imageio.IIOException;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class DatabaseImpl implements Database {


    public  String dbName;
    public  Path databaseRoot;

    public static Map<String, TableImpl> tableDictionary = new HashMap<String, TableImpl>();

    private DatabaseImpl(String dbName, Path databaseRoot)
    {
        this.dbName = dbName;
        this.databaseRoot = databaseRoot;
    }


    public static Database create(String dbName, Path databaseRoot) throws DatabaseException, IOException {

        if (dbName == null)
            throw new DatabaseException("Why dataBase name is null?");

        if(Files.exists(Paths.get(databaseRoot.toString(),dbName)))
            throw new DatabaseException("This" + dbName + "already exists");

        try
        {
            Files.createDirectory(Paths.get(databaseRoot.toString(),dbName));
        }
        catch(IOException ex)
        {
            throw new DatabaseException("Error while creating a DataBase("+dbName+") directory");
        }

        return new DatabaseImpl(dbName,databaseRoot);
    /**
     * @param databaseRoot путь к директории, которая может содержать несколько БД,
     *                     поэтому при создании БД необходимо создать директорию внутри databaseRoot.
     */
    public static Database create(String dbName, Path databaseRoot) throws DatabaseException {
        return null;
    }

    public static Database initializeFromContext(DatabaseInitializationContext context) {
        return null;
    }

    @Override
    public String getName() {

        return dbName;
    }

    @Override
    public void createTableIfNotExists(String tableName) throws DatabaseException, IOException {

        if (tableName == null)
            throw new DatabaseException("Why tableName name is null?");

        if ((tableDictionary.containsKey(tableName)) || (Files.exists(Paths.get(databaseRoot.toString(),dbName , tableName))))
            throw new DatabaseException("We have " + tableName + " in " +dbName+ "directory");

        TableIndex newTableIndex = new TableIndex();
        Path pathToTableRoot = Paths.get(databaseRoot.toString() , dbName);
        TableImpl newTable = (TableImpl) TableImpl.create(tableName , pathToTableRoot , newTableIndex);

        tableDictionary.put(tableName,newTable);
    }

    @Override
    public void write(String tableName, String objectKey, byte[] objectValue) throws DatabaseException {
        if (!tableDictionary.containsKey(tableName)){
            throw new DatabaseException("Table " + tableName + " doesn't exist in database" + dbName);
        }
       // TableImpl tableimpl = tableDictionary.get(tableName);

        if (tableName == null)
            throw new DatabaseException("Error while writing in database");
        TableImpl tableimpl = tableDictionary.get(tableName);
        tableimpl.write(objectKey ,  objectValue );

    }

    @Override
    public Optional<byte[]> read(String tableName, String objectKey) throws DatabaseException {

        TableImpl tableimpl = tableDictionary.get(tableName);

        if (tableName == null)
            throw new DatabaseException("Error while writing in database");

        return tableimpl.read(objectKey);

        //return Optional.empty();
    }

    @Override
    public void delete(String tableName, String objectKey) throws DatabaseException {

        TableImpl tableimpl = tableDictionary.get(tableName);

        if (tableName == null)
            throw new DatabaseException("Writing in database error");

        tableimpl.delete(objectKey);

    }
}
