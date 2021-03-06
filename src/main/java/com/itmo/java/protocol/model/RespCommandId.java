package com.itmo.java.protocol.model;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

/**
 * Id
 */
public class RespCommandId implements RespObject {

    /**
     * Код объекта
     */
    public static final byte CODE = '!';
    private final int commandId;

    public RespCommandId(int comId) {
        commandId = comId;
    }

    /**
     * Ошибка ли это? Ответ - нет
     *
     * @return false
     */
    @Override
    public boolean isError() {
        return false;
    }

    @Override
    public String asString() {
        return Integer.toString(commandId);
    }

    @Override
    public void write(OutputStream output) throws IOException {
        output.write(CODE);
        int commandByte = commandId;
        output.write((commandByte >>> 24) & 0xFF);
        output.write((commandByte >>> 16) & 0xFF);
        output.write((commandByte >>> 8) & 0xFF);
        output.write(commandByte &0xFF);
        output.write(CRLF);
    }
}
