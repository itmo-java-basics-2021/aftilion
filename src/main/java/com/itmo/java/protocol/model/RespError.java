package com.itmo.java.protocol.model;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

/**
 * Сообщение об ошибке в RESP протоколе
 */
public class RespError implements RespObject {

    /**
     * Код объекта
     */
    public static final byte CODE = '-';
    public byte[] message;

    public RespError(byte[] mes) {
        message = mes;
    }

    /**
     * Ошибка ли это? Ответ - да
     *
     * @return true
     */
    @Override
    public boolean isError() {
        return true;
    }

    @Override
    public String asString() {
        if (message == null) {
            return null;
        }
        return new String(message , StandardCharsets.UTF_8);
    }

    @Override
    public void write(OutputStream output) throws IOException {
        try {
            output.write(CODE);
            output.write(message);
            output.write(CRLF);
            output.flush();
        } catch(IOException ex) {
            throw new IOException(ex);
        }
    }
}
