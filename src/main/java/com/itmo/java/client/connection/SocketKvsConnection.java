package com.itmo.java.client.connection;

import com.itmo.java.basics.exceptions.DatabaseException;
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

    final Socket socket;
    final ConnectionConfig connectionConfig;

    public SocketKvsConnection(ConnectionConfig config) {
          try {
              connectionConfig = config;
              socket = new Socket(connectionConfig.toString(), connectionConfig.getPort());
          } catch (IOException ex) {
             throw  new RuntimeException("SocketKvsConnection" , ex);
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

            final RespWriter respWriter = new RespWriter(socket.getOutputStream());
            respWriter.write(command);
            final RespReader respReader = new RespReader(socket.getInputStream());
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
           // respReader.close();
          //  respWriter.close();
        } catch (IOException ex) {
          throw new RuntimeException("Socket close" , ex);
        }

    }
}
