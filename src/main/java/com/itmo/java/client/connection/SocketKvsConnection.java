package com.itmo.java.client.connection;

import com.itmo.java.client.exception.ConnectionException;
import com.itmo.java.protocol.RespReader;
import com.itmo.java.protocol.RespWriter;
import com.itmo.java.protocol.model.RespArray;
import com.itmo.java.protocol.model.RespCommandId;
import com.itmo.java.protocol.model.RespObject;

import java.io.IOException;
import java.net.Socket;

/**
 * С помощью {@link RespWriter} и {@link RespReader} читает/пишет в сокет
 */
public class SocketKvsConnection implements KvsConnection {

    private Socket socket;
    private RespReader respReader;
    //private RespWriter respWriter;

    public SocketKvsConnection(ConnectionConfig config) {
          try {
              socket = new Socket(config.toString(), config.getPort());
              respReader = new RespReader(socket.getInputStream());
              //respWriter = new RespWriter(socket.getOutputStream());
          } catch (IOException ex) {
              ex.toString();
          }
    }

    /**
     * Отправляет с помощью сокета команду и получает результат.
     *
     * @param commandId id команды (номер)
     * @param command   команда
     * @throws ConnectionException если сокет закрыт или если произошла другая ошибка соединения
     */
    @Override
    public synchronized RespObject send(int commandId, RespArray command) throws ConnectionException {
        try {
            RespWriter respWriter = new RespWriter(socket.getOutputStream());
            respWriter.write(command);
            RespObject respObject = respReader.readObject();
            if (respObject.isError()) {
                throw new ConnectionException("Connection error respObgect is error");
            }
            return  respObject;
        } catch (IOException ex) {
            throw new ConnectionException("Error while sendinh in SocketKvsConnection" , ex);
        }
    }

    /**
     * Закрывает сокет (и другие использованные ресурсы)
     */
    @Override
    public void close() {
        try {
            socket.close();
            respReader.close();
            respWriter.close();
        } catch (IOException ex) {
            ex.printStackTrace();
        }

    }
}
