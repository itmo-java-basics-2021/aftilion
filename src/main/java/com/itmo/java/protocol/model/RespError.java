package com.itmo.java.protocol.model;

import java.io.IOException;
import java.io.OutputStream;

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
        return new String(message);
    }

    @Override
    public void write(OutputStream output) throws IOException {
        try {
            output.write(CODE);
            output.write(message);
            output.write(CRLF);
        } catch(IOException ex) {
            throw new IOException(ex);
        }
    }
}