package com.itmo.java.client.client;

import com.itmo.java.basics.console.DatabaseCommandResult;
import com.itmo.java.client.command.*;
import com.itmo.java.client.connection.KvsConnection;
import com.itmo.java.client.exception.ConnectionException;
import com.itmo.java.client.exception.DatabaseExecutionException;
import com.itmo.java.protocol.model.RespObject;

import java.util.Arrays;
import java.util.function.Supplier;

public class SimpleKvsClient implements KvsClient {

    private final String dbName;
    private final KvsConnection connectionSupplier;

    /**
     * Конструктор
     *
     * @param databaseName       имя базы, с которой работает
     * @param connectionSupplier метод создания подключения к базе
     */
    public SimpleKvsClient(String databaseName, Supplier<KvsConnection> connectionSupplier) {
        dbName = databaseName;
        this.connectionSupplier = connectionSupplier.get();
    }

    @Override
    public String createDatabase() throws DatabaseExecutionException {
        CreateDatabaseKvsCommand dbKvsCom = new CreateDatabaseKvsCommand(dbName);
        try {
          //  CreateDatabaseKvsCommand dbKvsCom = new CreateDatabaseKvsCommand(dbName);
            RespObject obj = connectionSupplier.send(dbKvsCom.getCommandId(), dbKvsCom.serialize());
            if (obj.isError()) {
                throw new DatabaseExecutionException(obj.asString());
            }
            return obj.asString();
        } catch (ConnectionException ex) {
          //  throw new DatabaseExecutionException("Creating DataBAse SinpleKvsClient, ConnectionException", ex);
            throw new DatabaseExecutionException(String.format("DatabaseExecutionException "  + ex.getMessage() + Arrays.toString(ex.getStackTrace()),
                    dbKvsCom.serialize().asString()), ex);
        }
    }

    @Override
    public String createTable(String tableName) throws DatabaseExecutionException {
        CreateTableKvsCommand tbKvsCom = new CreateTableKvsCommand(dbName, tableName);
        try {
        //    CreateTableKvsCommand tbKvsCom = new CreateTableKvsCommand(dbName, tableName);
            RespObject obj = connectionSupplier.send(tbKvsCom.getCommandId(), tbKvsCom.serialize());
            if (obj.isError()) {
                throw new DatabaseExecutionException(obj.asString());
            }
            return obj.asString();
        } catch (ConnectionException ex) {
           // throw new DatabaseExecutionException("Creating table SinpleKvsClient, ConnectionException", ex);
            throw new DatabaseExecutionException(String.format("DatabaseExecutionException "  + ex.getMessage() + Arrays.toString(ex.getStackTrace()),
                    tbKvsCom.serialize().asString()), ex);
        }
    }

    @Override
    public String get(String tableName, String key) throws DatabaseExecutionException {
        GetKvsCommand getKvsCom = new GetKvsCommand(dbName, tableName, key);
        try {
         //   GetKvsCommand getKvsCom = new GetKvsCommand(dbName, tableName, key);
            RespObject obj = connectionSupplier.send(getKvsCom.getCommandId(), getKvsCom.serialize());
            if (obj.isError()) {
                throw new DatabaseExecutionException(obj.asString());
            }
            return obj.asString();
        } catch (ConnectionException ex) {
          //  throw new DatabaseExecutionException("getting SinpleKvsClient, ConnectionException", ex);
            throw new DatabaseExecutionException(String.format("DatabaseExecutionException "  + ex.getMessage() + Arrays.toString(ex.getStackTrace()),
                    getKvsCom.serialize().asString()), ex);
        }
    }

    @Override
    public String set(String tableName, String key, String value) throws DatabaseExecutionException {
        SetKvsCommand setKvsCom = new SetKvsCommand(dbName, tableName, key, value);
        try {
         //   SetKvsCommand setKvsCom = new SetKvsCommand(dbName, tableName, key, value);
            RespObject obj = connectionSupplier.send(setKvsCom.getCommandId(), setKvsCom.serialize());
            if (obj.isError()) {
                throw new DatabaseExecutionException(obj.asString());
            }
            return obj.asString();
        } catch (ConnectionException ex) {
         //   throw new DatabaseExecutionException("setting SinpleKvsClient, ConnectionException", ex);
            throw new DatabaseExecutionException(String.format("DatabaseExecutionException "  + ex.getMessage() + Arrays.toString(ex.getStackTrace()),
                    setKvsCom.serialize().asString()), ex);
        }
    }

    @Override
    public String delete(String tableName, String key) throws DatabaseExecutionException {
        DeleteKvsCommand delKvsCom = new DeleteKvsCommand(dbName, tableName, key);
        try {
           // DeleteKvsCommand delKvsCom = new DeleteKvsCommand(dbName, tableName, key);
            RespObject obj = connectionSupplier.send(delKvsCom.getCommandId(), delKvsCom.serialize());
            if (obj.isError()) {
                throw new DatabaseExecutionException(obj.asString());
            }
            return obj.asString();
        } catch (ConnectionException ex) {
//            throw new DatabaseExecutionException("Deleting SinpleKvsClient, ConnectionException", ex);
            throw new DatabaseExecutionException(String.format("DatabaseExecutionException "  + ex.getMessage() + Arrays.toString(ex.getStackTrace()),
                    delKvsCom.serialize().asString()), ex);
        }
    }
}