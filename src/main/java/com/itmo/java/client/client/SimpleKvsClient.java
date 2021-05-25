package com.itmo.java.client.client;

import com.itmo.java.basics.console.DatabaseCommandResult;
import com.itmo.java.client.command.*;
import com.itmo.java.client.connection.KvsConnection;
import com.itmo.java.client.exception.ConnectionException;
import com.itmo.java.client.exception.DatabaseExecutionException;
import com.itmo.java.protocol.model.RespObject;

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
        try {
            CreateDatabaseKvsCommand dbKvsCom = new CreateDatabaseKvsCommand(dbName);
            RespObject obj = connectionSupplier.send(dbKvsCom.getCommandId(), dbKvsCom.serialize());
            if (obj.isError()) {
                throw new DatabaseExecutionException(obj.asString());
            }
            return obj.asString();
        } catch (ConnectionException ex) {
            return DatabaseCommandResult.error(ex).getPayLoad();
        }
    }

    @Override
    public String createTable(String tableName) throws DatabaseExecutionException {
        try {
            CreateTableKvsCommand tbKvsCom = new CreateTableKvsCommand(dbName, tableName);
            RespObject obj = connectionSupplier.send(tbKvsCom.getCommandId(), tbKvsCom.serialize());
            if (obj.isError()) {
                throw new DatabaseExecutionException(obj.asString());
            }
            return obj.asString();
        } catch (ConnectionException ex) {
            return DatabaseCommandResult.error(ex).getPayLoad();
        }
    }

    @Override
    public String get(String tableName, String key) throws DatabaseExecutionException {
        try {
            GetKvsCommand getKvsCom = new GetKvsCommand(dbName, tableName, key);
            RespObject obj = connectionSupplier.send(getKvsCom.getCommandId(), getKvsCom.serialize());
            if (obj.isError()) {
                throw new DatabaseExecutionException(obj.asString());
            }
            return obj.asString();
        } catch (ConnectionException ex) {
            return DatabaseCommandResult.error(ex).getPayLoad();
        }
    }

    @Override
    public String set(String tableName, String key, String value) throws DatabaseExecutionException {
        try {
            SetKvsCommand setKvsCom = new SetKvsCommand(dbName, tableName, key, value);
            RespObject obj = connectionSupplier.send(setKvsCom.getCommandId(), setKvsCom.serialize());
            if (obj.isError()) {
                throw new DatabaseExecutionException(obj.asString());
            }
            return obj.asString();
        } catch (ConnectionException ex) {
            return DatabaseCommandResult.error(ex).getPayLoad();
        }
    }

    @Override
    public String delete(String tableName, String key) throws DatabaseExecutionException {
        try {
            DeleteKvsCommand delKvsCom = new DeleteKvsCommand(dbName, tableName, key);
            RespObject obj = connectionSupplier.send(delKvsCom.getCommandId(), delKvsCom.serialize());
            if (obj.isError()) {
                throw new DatabaseExecutionException(obj.asString());
            }
            return obj.asString();
        } catch (ConnectionException ex) {
            return DatabaseCommandResult.error(ex).getPayLoad();
        }
    }
}
