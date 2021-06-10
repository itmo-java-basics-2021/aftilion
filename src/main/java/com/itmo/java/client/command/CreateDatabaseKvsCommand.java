package com.itmo.java.client.command;

import com.itmo.java.protocol.model.RespArray;
import com.itmo.java.protocol.model.RespBulkString;
import com.itmo.java.protocol.model.RespCommandId;

import java.nio.charset.StandardCharsets;

/**
 * Команда для создания бд
 */
public class CreateDatabaseKvsCommand implements KvsCommand {

    private static final String COMMAND_NAME = "CREATE_DATABASE";
    private final String dbName;
    private final int dbID;

    /**
     * Создает объект
     *
     * @param databaseName имя базы данных
     */
    public CreateDatabaseKvsCommand(String databaseName) {
        dbID = idGen.incrementAndGet();
        dbName = databaseName;
    }

    /**
     * Возвращает RESP объект. {@link RespArray} с {@link RespCommandId}, именем команды, аргументами в виде {@link RespBulkString}
     *
     * @return объект
     */
    @Override
    public RespArray serialize() {
        return new RespArray(new RespCommandId(dbID),
                new RespBulkString(COMMAND_NAME.getBytes(StandardCharsets.UTF_8)),
                new RespBulkString(dbName.getBytes(StandardCharsets.UTF_8)));
    }

    @Override
    public int getCommandId() {
        return dbID;
    }
}