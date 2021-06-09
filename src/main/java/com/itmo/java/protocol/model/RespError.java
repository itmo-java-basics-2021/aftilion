package com.itmo.java.protocol.model;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

/**
 * Сообщение об ошибке в RESP протоколе
 */
public class RespError implements RespObject {
    private final byte[] message;

    /**
     * Код объекта
     */
    public static final byte CODE = '-';

    public RespError(byte[] message) {
        this.message = message;
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
        return new String(message, StandardCharsets.UTF_8);
    }

    @Override
    public void write(OutputStream os) throws IOException {
        os.write(CODE);
        os.write(message);
        os.write(CRLF);
        os.flush();
    }
}
