package com.itmo.java.basics.console.impl;

import com.itmo.java.basics.console.DatabaseCommandResult;
import com.itmo.java.protocol.model.RespBulkString;
import com.itmo.java.protocol.model.RespObject;

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
        if(payLoad != null) {
            return new String(payLoad);
        } else {
            return null;
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