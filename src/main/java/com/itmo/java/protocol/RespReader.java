package com.itmo.java.protocol;

import com.itmo.java.protocol.model.RespArray;
import com.itmo.java.protocol.model.RespBulkString;
import com.itmo.java.protocol.model.RespCommandId;
import com.itmo.java.protocol.model.RespError;
import com.itmo.java.protocol.model.RespObject;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class RespReader implements AutoCloseable {

    /**
     * Специальные символы окончания элемента
     */
    private static final byte CR = '\r';
    private static final byte LF = '\n';
    private final InputStream inputStream;

    public RespReader(InputStream is) {
       inputStream = is;
    }

    /**
     * Есть ли следующий массив в стриме?
     */
    public boolean hasArray() throws IOException {
        byte bytes = inputStream.readNBytes(1)[0];
        return bytes == RespArray.CODE;
    }

    /**
     * Считывает из input stream следующий объект. Может прочитать любой объект, сам определит его тип на основе кода объекта.
     * Например, если первый элемент "-", то вернет ошибку. Если "$" - bulk строку
     *
     * @throws EOFException если stream пустой
     * @throws IOException  при ошибке чтения
     */
    public RespObject readObject() throws IOException {
        if (inputStream == null) {
            throw new EOFException("Why stream is null?");
        }
        byte bytes = inputStream.readNBytes(1)[0];
        switch(bytes) {
            case RespError.CODE:
                return readError();
            case RespBulkString.CODE:
                return readBulkString();
            case RespCommandId.CODE:
                return readCommandId();
            case RespArray.CODE:
                return readArray();
            default:
                throw new IOException("Error while reading object code");
        }
    }

    /**
     * Считывает объект ошибки
     *
     * @throws EOFException если stream пустой
     * @throws IOException  при ошибке чтения
     */
    public RespError readError() throws IOException {
        if (inputStream == null) {
            throw new EOFException("Why stream is null?");
        }
        boolean stop = false;
        byte bytes = inputStream.readNBytes(1)[0];
        List<Byte> errorBytes = new ArrayList<>();
        while (!stop) {
            while ((bytes == CR)) {
                bytes = inputStream.readNBytes(1)[0];
                if (bytes == LF) {
                    stop = true;
                } else {
                    errorBytes.add(CR);
//                    errorBytes.add(bytes);
//                    bytes = inputStream.readNBytes(1)[0];
                }
            }
                if (!stop) {
                    errorBytes.add(bytes);
                    bytes = inputStream.readNBytes(1)[0];
                }
        }
        int errorBytesCount = errorBytes.size();
        byte[] errorByte = new byte[errorBytesCount];
        for (int i = 0; i < errorBytesCount; i++) {
            errorByte[i] = errorBytes.get(i);
        }
        return new RespError(errorByte);
//        return null;
    }

    /**
     * Читает bulk строку
     *
     * @throws EOFException если stream пустой
     * @throws IOException  при ошибке чтения
     */
    public RespBulkString readBulkString() throws IOException {
//        if (inputStream == null) {
//            throw new EOFException("Why stream is null?");
//        }
//        byte bytes = inputStream.readNBytes(1)[0];
//        List<Byte> bulkBytes = new ArrayList<>();
//        while ( bytes != CR) {
//            bulkBytes.add(bytes);
//            bytes = inputStream.readNBytes(1)[0];
//        }
//        int bulkBytesSize = bulkBytes.size();
//        byte[] bulkByte = new byte[bulkBytesSize];
//
//        for (int i = 0; i < bulkBytesSize; i++) {
//            bulkByte[i] = bulkBytes.get(i);
//        }
//        final int bulkByteCount = Integer.parseInt(new String( bulkByte, StandardCharsets.UTF_8));
//        inputStream.readNBytes(1);
//        if (bulkByteCount == RespBulkString.NULL_STRING_SIZE) {
//            return new RespBulkString(null);
//        }
//        byte[] data = inputStream.readNBytes(bulkByteCount);
//        inputStream.readNBytes(2);
//        return new RespBulkString(data);
        return null;
    }

    /**
     * Считывает массив RESP элементов
     *
     * @throws EOFException если stream пустой
     * @throws IOException  при ошибке чтения
     */
    public RespArray readArray() throws IOException {
//        if (inputStream == null) {
//            throw new EOFException("Why stream is null?");
//        }
//        byte bytes = inputStream.readNBytes(1)[0];
//        List<Byte> arrayBytes = new ArrayList<>();
//        while ( bytes != CR) {
//            arrayBytes.add(bytes);
//            bytes = inputStream.readNBytes(1)[0];
//        }
//        int arrayBytesSize = arrayBytes.size();
//        byte[] arrayByte = new byte[arrayBytesSize];
//
//        for (int i = 0; i < arrayBytesSize; i++) {
//            arrayByte[i] = arrayBytes.get(i);
//        }
//        final int arrayByteCount = Integer.parseInt(new String( arrayByte, StandardCharsets.UTF_8));
//        RespObject[] objects = new RespObject[arrayByteCount];
//        inputStream.readNBytes(1);
//        for (int i = 0; i < arrayByteCount; i++) {
//            objects[i] = this.readObject();
//        }
//        return new RespArray(objects);
        return null;
    }

    /**
     * Считывает id команды
     *
     * @throws EOFException если stream пустой
     * @throws IOException  при ошибке чтения
     */
    public RespCommandId readCommandId() throws IOException {
        int comID = ByteBuffer.wrap(inputStream.readNBytes(4)).getInt();
        inputStream.readNBytes(2);
        return new RespCommandId(comID);
    }


    @Override
    public void close() throws IOException {
        inputStream.close();
    }
}
