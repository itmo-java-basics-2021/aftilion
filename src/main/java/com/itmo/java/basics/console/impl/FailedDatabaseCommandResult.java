package com.itmo.java.basics.console.impl;

import com.itmo.java.basics.console.DatabaseCommandResult;
import com.itmo.java.protocol.model.RespError;
import com.itmo.java.protocol.model.RespObject;

import java.nio.charset.StandardCharsets;

/**
 * Зафейленная команда
 */
public class FailedDatabaseCommandResult implements DatabaseCommandResult {
    private final String payLoad;

    public FailedDatabaseCommandResult(String pload) {
        this.payLoad = pload;
    }

    /**
     * Сообщение об ошибке
     */
    @Override
    public String getPayLoad() {
        return payLoad;
    }

    @Override
    public boolean isSuccess() {
        return false;
    }

    /**
     * Сериализуется в {@link RespError}
     */
    @Override
    public RespObject serialize() {
        return new RespError(payLoad.getBytes(StandardCharsets.UTF_8));
    }
}
