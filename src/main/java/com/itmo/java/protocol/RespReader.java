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
    private final InputStream is;

    /**
     * Специальные символы окончания элемента
     */
    private static final byte CR = '\r';
    private static final byte LF = '\n';

    public RespReader(InputStream is) {
        this.is = is;
    }

    /**
     * Есть ли следующий массив в стриме?
     */
    public boolean hasArray() throws IOException {
        final byte code = is.readNBytes(1)[0];

        return code == RespArray.CODE;
    }

    /**
     * Считывает из input stream следующий объект. Может прочитать любой объект, сам определит его тип на основе кода объекта.
     * Например, если первый элемент "-", то вернет ошибку. Если "$" - bulk строку
     *
     * @throws EOFException если stream пустой
     * @throws IOException  при ошибке чтения
     */
    public RespObject readObject() throws IOException {
        try {
            byte[] firstSymbol = is.readNBytes(1);
            if (firstSymbol.length == 0){
                throw new EOFException("end of the stream");
            }
            if (firstSymbol[0] == RespError.CODE){
                return readError();
            }
            if(firstSymbol[0] == RespBulkString.CODE){
                return readBulkString();
            }
            if(firstSymbol[0] == RespCommandId.CODE){
                return readCommandId();
            }
            throw new IOException("unknown byte" + new String(firstSymbol));
        } catch (IOException e){
            throw new IOException("exception in reading object", e);
        }
    }

    /**
     * Считывает объект ошибки
     *
     * @throws EOFException если stream пустой
     * @throws IOException  при ошибке чтения
     */
    public RespError readError() throws IOException {
        byte symbol = is.readNBytes(1)[0];

        final List<Byte> symbols = new ArrayList<>();

        boolean isEndOfError = false;

        while (!isEndOfError) {
            while (symbol == CR) {
                symbol = is.readNBytes(1)[0];

                if (symbol == LF) {
                    isEndOfError = true;
                } else {
                    symbols.add(CR);
                }
            }

            if (!isEndOfError) {
                symbols.add(symbol);
                symbol = is.readNBytes(1)[0];
            }
        }

        final int symbolsCount = symbols.size();

        final byte[] bytes = new byte[symbolsCount];

        for (int i = 0; i < symbolsCount; i++) {
            bytes[i] = symbols.get(i);
        }

        return new RespError(bytes);
    }

    /**
     * Читает bulk строку
     *
     * @throws EOFException если stream пустой
     * @throws IOException  при ошибке чтения
     */
    public RespBulkString readBulkString() throws IOException {
        final byte[] bytes = this.getStringNumberInBytes();
        final int bytesCount = Integer.parseInt(new String(bytes, StandardCharsets.UTF_8));

        is.readNBytes(1);

        if (bytesCount == RespBulkString.NULL_STRING_SIZE) {
            return RespBulkString.NULL_STRING;
        }

        final byte[] data = is.readNBytes(bytesCount);

        is.readNBytes(2);

        return new RespBulkString(data);
    }

    /**
     * Считывает массив RESP элементов
     *
     * @throws EOFException если stream пустой
     * @throws IOException  при ошибке чтения
     */
    public RespArray readArray() throws IOException {
        final byte[] bytes = this.getStringNumberInBytes();
        final int elementsCount = Integer.parseInt(new String(bytes, StandardCharsets.UTF_8));

        final RespObject[] objects = new RespObject[elementsCount];

        is.readNBytes(1);

        for (int i = 0; i < elementsCount; i++) {
            objects[i] = this.readObject();
        }

        return new RespArray(objects);
    }

    /**
     * Считывает id команды
     *
     * @throws EOFException если stream пустой
     * @throws IOException  при ошибке чтения
     */
    public RespCommandId readCommandId() throws IOException {
        final int commandId = ByteBuffer.wrap(is.readNBytes(4)).getInt();

        is.readNBytes(2);

        return new RespCommandId(commandId);
    }


    @Override
    public void close() throws IOException {
        is.close();
    }

    private byte[] getStringNumberInBytes() throws IOException {
        byte symbol = is.readNBytes(1)[0];

        final List<Byte> symbols = new ArrayList<>();

        while (symbol != CR) {
            symbols.add(symbol);
            symbol = is.readNBytes(1)[0];
        }

        final int symbolsCount = symbols.size();

        final byte[] bytes = new byte[symbolsCount];

        for (int i = 0; i < symbolsCount; i++) {
            bytes[i] = symbols.get(i);
        }

        return bytes;
    }
}
