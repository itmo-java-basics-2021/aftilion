package com.itmo.java.client.command;

import com.itmo.java.protocol.model.RespArray;
import com.itmo.java.protocol.model.RespBulkString;
import com.itmo.java.protocol.model.RespCommandId;

import java.nio.charset.StandardCharsets;

public class DeleteKvsCommand implements KvsCommand {

    private static final String COMMAND_NAME = "DELETE_KEY";
    private final String dbName;
    private final String tbName;
    private final String Key;
    private final int delID;

    public DeleteKvsCommand(String databaseName, String tableName, String key) {
        dbName = databaseName;
        tbName = tableName;
        Key = key;
        delID = idGen.get();
    }

    /**
     * Возвращает RESP объект. {@link RespArray} с {@link RespCommandId}, именем команды, аргументами в виде {@link RespBulkString}
     *
     * @return объект
     */
    @Override
    public RespArray serialize() {
        return new RespArray(new RespCommandId(delID),
                new RespBulkString(COMMAND_NAME.getBytes(StandardCharsets.UTF_8)),
                new RespBulkString(dbName.getBytes(StandardCharsets.UTF_8)),
                new RespBulkString(tbName.getBytes(StandardCharsets.UTF_8)),
                new RespBulkString(Key.getBytes(StandardCharsets.UTF_8)));
    }

    @Override
    public int getCommandId() {
        return delID;
    }
}
