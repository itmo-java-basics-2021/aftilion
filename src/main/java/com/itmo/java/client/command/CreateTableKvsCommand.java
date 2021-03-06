package com.itmo.java.client.command;

import com.itmo.java.protocol.model.RespArray;
import com.itmo.java.protocol.model.RespBulkString;
import com.itmo.java.protocol.model.RespCommandId;

import java.nio.charset.StandardCharsets;

/**
 * Команда для создания таблицы
 */
public class CreateTableKvsCommand implements KvsCommand {

    private static final String COMMAND_NAME = "CREATE_TABLE";
    private final String dbName;
    private final int tbID;
    private final String tbName;

    public CreateTableKvsCommand(String databaseName, String tableName) {
        dbName = databaseName;
        tbName = tableName;
        tbID = idGen.get();
    }

    /**
     * Возвращает RESP объект. {@link RespArray} с {@link RespCommandId}, именем команды, аргументами в виде {@link RespBulkString}
     *
     * @return объект
     */
    @Override
    public RespArray serialize() {
        return new RespArray(new RespCommandId(tbID),
                new RespBulkString(COMMAND_NAME.getBytes(StandardCharsets.UTF_8)),
                new RespBulkString(dbName.getBytes(StandardCharsets.UTF_8)),
                new RespBulkString(tbName.getBytes(StandardCharsets.UTF_8)));
    }

    @Override
    public int getCommandId() {
        return tbID;
    }
}
