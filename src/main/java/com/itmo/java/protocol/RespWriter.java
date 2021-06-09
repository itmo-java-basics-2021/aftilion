package com.itmo.java.protocol;

import com.itmo.java.protocol.model.RespObject;

import java.io.IOException;
import java.io.OutputStream;

public class RespWriter implements AutoCloseable{

    private final OutputStream outputStream;

    public RespWriter(OutputStream os) {

        outputStream = os;
    }

    /**
     * Записывает в output stream объект
     */
    public void write(RespObject object) throws IOException {
        try {
            object.write(outputStream);
        } catch (IOException ex) {
           ex.printStackTrace();
        }
    }

    @Override
    public void close() throws IOException {
        try {
            outputStream.close();
        } catch (IOException ex) {
           ex.printStackTrace();
        }
    }
}