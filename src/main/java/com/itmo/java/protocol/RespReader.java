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
    private boolean isHasArray = false;

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
        try{
            int bulkSize = readInt();
            readCompareByte(LF);
            if (bulkSize == -1){
                return RespBulkString.NULL_STRING;
            }
            byte[] bulkString = is.readNBytes(bulkSize);
            readCompareByte(CR);
            readCompareByte(LF);
            return new RespBulkString(bulkString);
        } catch (IOException e) {
            throw new IOException("IO exception in reading Bulk String", e);
        }

    }

    /**
     * Считывает массив RESP элементов
     *
     * @throws EOFException если stream пустой
     * @throws IOException  при ошибке чтения
     */
    public RespArray readArray() throws IOException {
        try {
            if (!hasArray()) {
                readCompareByte(RespArray.CODE);
            }
            int arraySize = readInt();
            readCompareByte(LF);
            RespObject[] listObjects = new RespObject[arraySize];
            for (int i = 0; i < arraySize; i++){
                listObjects[i] = readObject();
            }
            return new RespArray(listObjects);
        } catch (IOException e) {
            throw new IOException("IO exception in reading Array", e);
        }

    }

    /**
     * Считывает id команды
     *
     * @throws EOFException если stream пустой
     * @throws IOException  при ошибке чтения
     */
    public RespCommandId readCommandId() throws IOException {
        try {
            byte[] commandId1 = is.readNBytes(1);
            byte[] commandId2 = is.readNBytes(1);
            byte[] commandId3 = is.readNBytes(1);
            byte[] commandId4 = is.readNBytes(1);
            int commandId = ((commandId1[0] << 24) + (commandId2[0]<< 16) + (commandId3[0] << 8) + (commandId4[0] << 0));
            readCompareByte(CR);
            readCompareByte(LF);
            return new RespCommandId(commandId);
        } catch (IOException e) {
            throw new IOException("IO exception in reading command id", e);
        }
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

    private int readInt() throws IOException {
        try{
            StringBuilder size = new StringBuilder();
            byte[] sizeByte = is.readNBytes(1);
            if (sizeByte.length == 0){
                throw new EOFException("end of the stream");
            }
            while (sizeByte[0] != CR){
                size.append(new String(sizeByte));
                sizeByte = is.readNBytes(1);
                if (sizeByte.length == 0){
                    throw new EOFException("end of the stream");
                }
            }
            try {
                return Integer.parseInt(size.toString());
            } catch (NumberFormatException e) {
                throw new IOException("expected reading int from this string: " + size.toString());
            }
        } catch (IOException e) {
            throw new IOException("IO exception in reading int", e);
        }
    }

    private void readCompareByte(byte compareWith) throws IOException {
        byte[] nextByte;
        try {
            nextByte = is.readNBytes(1);
            if (nextByte.length == 0){
                throw new EOFException("end of the stream");
            } else if (nextByte[0] != compareWith) {
                throw new IOException("expected symbol:  " + String.valueOf(compareWith) + " but get: " + String.valueOf(nextByte[0]));
            }
        } catch (IOException e) {
            throw new IOException("IO exception in reading byte", e);
        }
    }

}