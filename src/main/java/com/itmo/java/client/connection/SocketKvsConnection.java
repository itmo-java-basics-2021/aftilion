package com.itmo.java.client.connection;


import com.itmo.java.client.exception.ConnectionException;
import com.itmo.java.protocol.RespReader;
import com.itmo.java.protocol.RespWriter;
import com.itmo.java.protocol.model.RespArray;
import com.itmo.java.protocol.model.RespObject;

import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;

/**
 * С помощью {@link RespWriter} и {@link RespReader} читает/пишет в сокет
 */
public class SocketKvsConnection implements KvsConnection {
    //    private final int port;
//    private final String host;
//    private final Socket clientSocket;
//    private final RespWriter respWriter;
//    private final RespReader respReader;
    final ConnectionConfig connectionConfig;
    final  Socket socket;
    public SocketKvsConnection(ConnectionConfig config) {
        connectionConfig = config;
        try {
            socket = new Socket(connectionConfig.getHost(),connectionConfig.getPort());
        } catch (IOException e) {
            throw new RuntimeException("Error in const");
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
            RespObject respObject = respReader.readObject();
            if (respObject.isError()) {
                throw new ConnectionException("Response error");
            }
            return respObject;
        } catch (IOException e) {
            close();
            return null;
            //throw new ConnectionException("IOException when send SocketKvs");
        }
    }

    /**
     * Закрывает сокет (и другие использованные ресурсы)
     */
    @Override
    public void close() {
        try {
            socket.close();
        } catch (IOException e) {
            throw new RuntimeException("IOException when try to close client socket");
        }
    }
}