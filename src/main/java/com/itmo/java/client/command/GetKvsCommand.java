package com.itmo.java.client.command;

import com.itmo.java.protocol.model.RespArray;
import com.itmo.java.protocol.model.RespBulkString;
import com.itmo.java.protocol.model.RespCommandId;

import java.nio.charset.StandardCharsets;

public class GetKvsCommand implements KvsCommand {

    private static final String COMMAND_NAME = "GET_KEY";
    private final String dbName;
    private final String tbName;
    private final String Key;
    private final int getID ;

    public GetKvsCommand(String databaseName, String tableName, String key) {
        dbName =databaseName;
        tbName = tableName;
        Key = key;
        getID = idGen.get();
    }

    /**
     * Возвращает RESP объект. {@link RespArray} с {@link RespCommandId}, именем команды, аргументами в виде {@link RespBulkString}
     *
     * @return объект
     */
    @Override
    public RespArray serialize() {
        return new RespArray(new RespCommandId(getID),
                new RespBulkString(COMMAND_NAME.getBytes(StandardCharsets.UTF_8)),
                new RespBulkString(dbName.getBytes(StandardCharsets.UTF_8)),
                new RespBulkString(tbName.getBytes(StandardCharsets.UTF_8)),
                new RespBulkString(Key.getBytes(StandardCharsets.UTF_8)));
    }

    @Override
    public int getCommandId() {
        return getID;
    }
}
