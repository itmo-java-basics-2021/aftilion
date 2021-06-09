package com.itmo.java.client.connection;

import com.itmo.java.basics.DatabaseServer;
import com.itmo.java.client.exception.ConnectionException;
import com.itmo.java.protocol.model.RespArray;
import com.itmo.java.protocol.model.RespCommandId;
import com.itmo.java.protocol.model.RespObject;

import java.util.concurrent.ExecutionException;

/**
 * Реализация подключения, когда есть прямая ссылка на объект
 * (пока еще нет реализации сокетов)
 */
public class DirectReferenceKvsConnection implements KvsConnection {
    private final DatabaseServer dbServer;

    public DirectReferenceKvsConnection(DatabaseServer databaseServer) {
        dbServer = databaseServer;
    }

    @Override
    public RespObject send(int commandId, RespArray command) throws ConnectionException {
        try {
            return dbServer.executeNextCommand(command).get().serialize();
        } catch (InterruptedException e) {
            throw new ConnectionException("ConnectionException when try to get result from server because of interruption when message is '" +
                    command.asString() + "'", e);
        } catch (ExecutionException e) {
            throw new ConnectionException("ConnectionException when try to get result from server because of ExecutionException when message is '" +
                    command.asString() + "'", e);
        }
    }

    /**
     * Ничего не делает ¯\_(ツ)_/¯
     */
    @Override
    public void close() {
    }
}
