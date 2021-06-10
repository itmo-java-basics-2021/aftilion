package com.itmo.java.protocol;

import com.itmo.java.protocol.model.RespArray;
import com.itmo.java.protocol.model.RespBulkString;
import com.itmo.java.protocol.model.RespCommandId;
import com.itmo.java.protocol.model.RespError;
import com.itmo.java.protocol.model.RespObject;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;


public class RespReader implements AutoCloseable {

    /**
     * Специальные символы окончания элемента
     */
    private static final byte CR = '\r';
    private static final byte LF = '\n';

    private final InputStream is;
    private String rawData = "";

    public RespReader(InputStream is) {
        this.is = is;
        try {
            rawData = new String(is.readNBytes(4));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Есть ли следующий массив в стриме?
     */
    public boolean hasArray() throws IOException {
        return rawData.startsWith("*");
    }

    /**
     * Считывает из input stream следующий объект. Может прочитать любой объект, сам определит его тип на основе кода объекта.
     * Например, если первый элемент "-", то вернет ошибку. Если "$" - bulk строку
     *
     * @throws EOFException если stream пустой
     * @throws IOException  при ошибке чтения
     */
    public RespObject readObject() throws IOException {
        if (rawData.isEmpty()) {
            throw new EOFException("Stream is empty");
        }

        if(rawData.startsWith("-")) {
            return readError();
        } else if (rawData.startsWith("$")) {
            return readBulkString();
        } else if (rawData.startsWith("!")) {
            return readCommandId();
        } else if (rawData.startsWith("*")){
            return readArray();
        } else {
            throw new IOException("Error while reading");
        }
    }

    /**
     * Считывает объект ошибки
     *
     * @throws EOFException если stream пустой
     * @throws IOException  при ошибке чтения
     */
    public RespError readError() throws IOException {
        if (rawData.length() < 3) {
            throw new EOFException("Length of rawData is too small");
        }
        String message = rawData.substring(1, rawData.length() - 2);
        return new RespError(message.getBytes());


    }

    /**
     * Читает bulk строку
     *
     * @throws EOFException если stream пустой
     * @throws IOException  при ошибке чтения
     */
    public RespBulkString readBulkString() throws IOException {
        if (rawData.length() < 3) {
            throw new EOFException("Length of rawData is too small");
        }
        String body = rawData.substring(1, rawData.length() - 2);
        String[] split = body.split("\r\n");
        if (split.length != 2) {
            throw new IOException("Error while reading");
        }
        byte [] data = split[1].getBytes();
        RespBulkString respBulkString = new RespBulkString(data);
        return respBulkString;




    }

    /**
     * Считывает массив RESP элементов
     *
     * @throws EOFException если stream пустой
     * @throws IOException  при ошибке чтения
     */
    public RespArray readArray() throws IOException {
        List<RespObject> respObjectList = new ArrayList<>();

        if (rawData.length() < 4) {
            throw new EOFException("Length of rawData is less then 4");
        }
        //String body = rawData.substring(1, rawData.length() - 2);
        String body = rawData.substring(1);
        String[] split = body.split("\r\n");
        int size = Integer.parseInt(split[0]);
        String arrayData = body.substring(split[0].length() + 2);

        //String[] elements = arrayData.split("[-$*]");
        for (int i = 0; i < size; i++) {
            //String element;
            if (arrayData.startsWith("-")) {
                arrayData = arrayData.substring(1);
                int currentIndex = arrayData.indexOf("\r\n");
                byte[] message = arrayData.substring(0, currentIndex).getBytes();
                respObjectList.add(new RespError(message));
                arrayData = arrayData.substring(currentIndex + 2);

            } else if (arrayData.startsWith("!")) {
                arrayData = arrayData.substring(1);
                int currentIndex = arrayData.indexOf("\r\n");
                String messageString = arrayData.substring(0, currentIndex);
                currentIndex = arrayData.indexOf("\r\n");
                byte[] dataBytes = messageString.substring(0, currentIndex).getBytes();
                int data = ByteBuffer.wrap(dataBytes).getInt();
                //int data = Integer.parseInt(arrayData.substring(0, currentIndex)); //?
                respObjectList.add(new RespCommandId(data));
                arrayData = arrayData.substring(currentIndex + 2);

            } else if (arrayData.startsWith("$")) {
                int currentIndex = arrayData.indexOf("\r\n");
                arrayData = arrayData.substring(currentIndex + 2);
                currentIndex = arrayData.indexOf("\r\n");
                byte[] data = arrayData.substring(0, currentIndex).getBytes();
                respObjectList.add(new RespBulkString(data));
                arrayData = arrayData.substring(currentIndex + 2);
            } else {
                throw new IOException("Error while reading");
            }

        }

        RespObject[] respArray = new RespObject[respObjectList.size()];
        respObjectList.toArray(respArray);
        return new RespArray(respArray);
    }

    /**
     * Считывает id команды
     *
     * @throws EOFException если stream пустой
     * @throws IOException  при ошибке чтения
     */
    public RespCommandId readCommandId() throws IOException {
        if (rawData.length() <= 3) {
            throw new EOFException("Length of rawData is too small");
        }
        String message = rawData.substring(1, rawData.length() - 2);
        byte[] messageBytes = message.getBytes();
        return new RespCommandId(ByteBuffer.wrap(messageBytes).getInt());
        //return new RespCommandId(Integer.parseInt(message));
    }


    @Override
    public void close() throws IOException {
        is.close();
    }
}
