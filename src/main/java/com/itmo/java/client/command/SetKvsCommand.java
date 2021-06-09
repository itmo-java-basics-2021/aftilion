package com.itmo.java.client.command;

import com.itmo.java.protocol.model.RespArray;
import com.itmo.java.protocol.model.RespBulkString;
import com.itmo.java.protocol.model.RespCommandId;

import java.nio.charset.StandardCharsets;

public class SetKvsCommand implements KvsCommand {

    private static final String COMMAND_NAME = "SET_KEY";
    private final String dbName;
    private final String tbName;
    private final String Key;
    private final int setID;
    private final String Value;

    public SetKvsCommand(String databaseName, String tableName, String key, String value) {
        dbName = databaseName;
        tbName = tableName;
        Key = key;
        setID = idGen.incrementAndGet();
        Value = value;
    }

    /**
     * Возвращает RESP объект. {@link RespArray} с {@link RespCommandId}, именем команды, аргументами в виде {@link RespBulkString}
     *
     * @return объект
     */
    @Override
    public RespArray serialize() {
        return new RespArray(new RespCommandId(setID),
                new RespBulkString(COMMAND_NAME.getBytes(StandardCharsets.UTF_8)),
                new RespBulkString(dbName.getBytes(StandardCharsets.UTF_8)),
                new RespBulkString(tbName.getBytes(StandardCharsets.UTF_8)),
                new RespBulkString(Key.getBytes(StandardCharsets.UTF_8)),
                new RespBulkString(Value.getBytes(StandardCharsets.UTF_8)));
    }

    @Override
    public int getCommandId() {
        return setID;
    }
}
