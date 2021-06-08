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
        try {
            byte bytes = inputStream.readNBytes(1)[0];
            return bytes == RespArray.CODE;
        } catch (IOException ex) {
            throw new IOException("has array" ,ex);
        }
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
        try {
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
        }catch(IOException ex) {
            throw new IOException("readobject" ,ex);
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
        try {
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
        } catch(IOException ex) {
            throw new IOException("read error",ex);
        }
    }

    /**
     * Читает bulk строку
     *
     * @throws EOFException если stream пустой
     * @throws IOException  при ошибке чтения
     */
    public RespBulkString readBulkString() throws IOException {
        if (inputStream == null) {
            throw new EOFException("Why stream is null?");
        }
        try {
            final byte[] bytes = this.getStringNumberInBytes();
            final int bytesCount = Integer.parseInt(new String(bytes, StandardCharsets.UTF_8));
            inputStream.readNBytes(1);
            if (bytesCount == RespBulkString.NULL_STRING_SIZE) {
                return RespBulkString.NULL_STRING;
            }
            final byte[] data = inputStream.readNBytes(bytesCount);
            inputStream.readNBytes(2);
            return new RespBulkString(data);
        } catch (IOException ex) {
            throw new IOException("RespBulk " ,ex);
        }
    }

    /**
     * Считывает массив RESP элементов
     *
     * @throws EOFException если stream пустой
     * @throws IOException  при ошибке чтения
     */
    public RespArray readArray() throws IOException {
        if (inputStream == null) {
            throw new EOFException("Why stream is null?");
        }
        try {
            final byte[] bytes = this.getStringNumberInBytes();
            final int elementsCount = Integer.parseInt(new String(bytes, StandardCharsets.UTF_8));
            final RespObject[] objects = new RespObject[elementsCount];
            inputStream.readNBytes(1);
            for (int i = 0; i < elementsCount; i++) {
                objects[i] = this.readObject();
            }
            return new RespArray(objects);
        } catch (IOException ex) {
            throw new IOException("readArray " ,ex);
        }
    }

    /**
     * Считывает id команды
     *
     * @throws EOFException если stream пустой
     * @throws IOException  при ошибке чтения
     */
    public RespCommandId readCommandId() throws IOException {
        if (inputStream == null) {
            throw new EOFException("Why stream is null?");
        }
        try {
            int comID = ByteBuffer.wrap(inputStream.readNBytes(4)).getInt();
            inputStream.readNBytes(2);
            return new RespCommandId(comID);
        } catch (IOException ex) {
            throw new IOException("readCommandID " ,ex);
        }
    }


    @Override
    public void close() throws IOException {
        try {
            inputStream.close();
        } catch (IOException ex) {
            throw new IOException("close " ,ex);
        }
    }

    private byte[] getStringNumberInBytes() throws IOException {
        try {
            byte symbol = inputStream.readNBytes(1)[0];
            final List<Byte> symboles = new ArrayList<>();
            while (symbol != CR) {
                symboles.add(symbol);
                symbol = inputStream.readNBytes(1)[0];
            }
            final int symbolsCount = symboles.size();
            final byte[] bytes = new byte[symbolsCount];
            for (int i = 0; i < symbolsCount; i++) {
                bytes[i] = symboles.get(i);
            }
            return bytes;
        } catch (IOException ex) {
            throw new IOException("getbytes", ex);
        }
    }
}