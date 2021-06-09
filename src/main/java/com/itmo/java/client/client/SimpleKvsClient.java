package com.itmo.java.client.client;

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
        this.dbName = databaseName;
        this.connectionSupplier = connectionSupplier.get();
    }

    @Override
    public String createDatabase() throws DatabaseExecutionException {
        KvsCommand command = new CreateDatabaseKvsCommand(dbName);
        return tryToSend(command);
    }

    @Override
    public String createTable(String tableName) throws DatabaseExecutionException {
        KvsCommand command = new CreateTableKvsCommand(dbName, tableName);
        return tryToSend(command);
    }

    @Override
    public String get(String tableName, String key) throws DatabaseExecutionException {
        KvsCommand command = new GetKvsCommand(dbName, tableName, key);
        return tryToSend(command);
    }

    @Override
    public String set(String tableName, String key, String value) throws DatabaseExecutionException {
        KvsCommand command = new SetKvsCommand(dbName, tableName, key, value);
        return tryToSend(command);
    }

    @Override
    public String delete(String tableName, String key) throws DatabaseExecutionException {
        KvsCommand command = new DeleteKvsCommand(dbName, tableName, key);
        return tryToSend(command);
    }

    private String tryToSend(KvsCommand command) throws DatabaseExecutionException {
        try {
            RespObject result = connectionSupplier.send(command.getCommandId(), command.serialize());
            if (result.isError()) {
                throw new DatabaseExecutionException(result.asString());
            }
            return result.asString();
        } catch (ConnectionException e) {
            throw new DatabaseExecutionException(String.format("DatabaseExecutionException when try to send '%s' with kvsConnection" + " Message:: " + e.getMessage() + " StackTrace:: " + Arrays.toString(e.getStackTrace()),
                    command.serialize().asString()), e);
        }
    }
}
