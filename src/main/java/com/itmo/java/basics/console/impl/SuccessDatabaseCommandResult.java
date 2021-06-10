package com.itmo.java.basics.console.impl;

import com.itmo.java.basics.console.DatabaseCommandResult;
import com.itmo.java.protocol.model.RespBulkString;
import com.itmo.java.protocol.model.RespObject;

import java.nio.charset.StandardCharsets;

/**
 * Результат успешной команды
 */
public class SuccessDatabaseCommandResult implements DatabaseCommandResult {
    private final byte[] payLoad;

    public SuccessDatabaseCommandResult(byte[] pload) {
        payLoad = pload;
    }

    @Override
    public String getPayLoad() {
        if (payLoad == null) {
            return null;
        } else {
            return new String(payLoad, StandardCharsets.UTF_8);
        }
    }

    @Override
    public boolean isSuccess() {
        return true;
    }

    /**
     * Сериализуется в {@link RespBulkString}
     */
    @Override
    public RespObject serialize() {
        return new RespBulkString(payLoad);
    }
}
