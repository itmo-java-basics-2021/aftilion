package com.itmo.java.protocol.model;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

/**
 * Строка
 */
public class RespBulkString implements RespObject {
    /**
     * Код объекта
     */
    public static final byte CODE = '$';
    public static final int NULL_STRING_SIZE = -1;
    private final byte[] data;


    public RespBulkString(byte[] inform) {
        data = inform;
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

    /**
     * Строковое представление
     *
     * @return строку, если данные есть. Если нет - null
     */
    @Override
    public String asString() {
        return new String(data);
    }

    @Override
    public void write(OutputStream output) throws IOException {
        output.write(CODE);
        output.write(Integer.toString(data.length).getBytes(StandardCharsets.UTF_8));
        output.write(CRLF);
        output.write(data);
    }
}
