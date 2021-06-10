package com.itmo.java.protocol.model;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Массив RESP объектов
 */
public class RespArray implements RespObject {

    /**
     * Код объекта
     */
    public static final byte CODE = '*';
    private List<RespObject> objects;

    public RespArray(RespObject... obj) {
        objects = Arrays.asList(obj);
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
     * @return результаты метода {@link RespObject#asString()} для всех хранимых объектов, разделенные пробелом
     */
    @Override
    public String asString() {
        return objects.stream().map(RespObject::asString).collect(Collectors.joining(" "));
    } // check

    @Override
    public void write(OutputStream output) throws IOException {
        try {
            output.write(CODE);
            output.write(Integer.toString(objects.size()).getBytes(StandardCharsets.UTF_8));
            output.write(CRLF);
            for (RespObject obj : objects) {
                obj.write(output);
            }
        } catch (IOException ex){
            throw new IOException(ex);
        }
    }

    public List<RespObject> getObjects() {
        return objects;
    }
}