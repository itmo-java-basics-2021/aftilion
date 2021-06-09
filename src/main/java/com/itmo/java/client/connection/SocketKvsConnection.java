package com.itmo.java.client.connection;

import com.itmo.java.client.exception.ConnectionException;
import com.itmo.java.protocol.RespReader;
import com.itmo.java.protocol.RespWriter;
import com.itmo.java.protocol.model.RespArray;
import com.itmo.java.protocol.model.RespObject;

import java.io.IOException;
import java.net.Socket;

/**
 * С помощью {@link RespWriter} и {@link RespReader} читает/пишет в сокет
 */
public class SocketKvsConnection implements KvsConnection {
    //final ConnectionConfig config;
    final Socket socket;
    final int port;
    final String host;
    final RespWriter respWriter;
    final RespReader respReader;

    public SocketKvsConnection(ConnectionConfig config) {
        port = config.getPort();
        host = config.getHost();

        try {
            socket = new Socket(host, port);
            respWriter = new RespWriter(socket.getOutputStream());
            respReader = new RespReader(socket.getInputStream());
        } catch (IOException exception) {
            throw new RuntimeException("Creation socket error", exception);
        }
    }

    /**
     * Отправляет с помощью сокета команду и получает результат.
     * @param commandId id команды (номер)
     * @param command   команда
     * @throws ConnectionException если сокет закрыт или если произошла другая ошибка соединения
     */
    @Override
    public synchronized RespObject send(int commandId, RespArray command) throws ConnectionException {
        try {
            RespWriter respWriter = new RespWriter(socket.getOutputStream());

            respWriter.write(command);

            RespReader respReader = new RespReader(socket.getInputStream());
//            final RespObject respObject = respReader.readObject();
//
//            if (respObject.isError()) {
//                throw new ConnectionException("Response error");
//            }

            return respReader.readObject();
        } catch (IOException exception) {
            throw new ConnectionException("Connection exception", exception);
        }
    }

    /**
     * Закрывает сокет (и другие использованные ресурсы)
     */
    @Override
    public void close() {
        try {
            respWriter.close();
            respReader.close();
            socket.close();
        } catch (IOException exception) {
            throw new RuntimeException("Closing client socket error", exception);
        }
    }
}