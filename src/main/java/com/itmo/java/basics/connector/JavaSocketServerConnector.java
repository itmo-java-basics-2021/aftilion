package com.itmo.java.basics.connector;

import com.itmo.java.basics.DatabaseServer;
import com.itmo.java.basics.config.ServerConfig;
import com.itmo.java.basics.console.DatabaseCommand;
import com.itmo.java.basics.resp.CommandReader;
import com.itmo.java.protocol.RespReader;
import com.itmo.java.protocol.RespWriter;
import lombok.SneakyThrows;

import java.io.Closeable;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Класс, который предоставляет доступ к серверу через сокеты
 */
public class JavaSocketServerConnector implements Closeable {

    /**
     * Экзекьютор для выполнения ClientTask
     */
    private final ExecutorService clientIOWorkers = Executors.newSingleThreadExecutor();
    private final ServerSocket serverSocket; // todo uncomment
    private final ExecutorService connectionAcceptorExecutor = Executors.newSingleThreadExecutor();
    private final DatabaseServer dbServer;

    /**
     * Стартует сервер. По аналогии с сокетом открывает коннекшн в конструкторе.
     */
    public JavaSocketServerConnector(DatabaseServer databaseServer, ServerConfig config) throws IOException {
        try {
            serverSocket = new ServerSocket(config.getPort());
            dbServer = databaseServer;
        } catch (IOException ex) {
            throw new IOException("Constructor",ex);
        }
    }

    /**
     * Начинает слушать заданный порт, начинает аксептить клиентские сокеты. На каждый из них начинает клиентскую таску
     */
    public void start() {
        connectionAcceptorExecutor.submit(() -> {
            try {
                final Socket socketClient = serverSocket.accept();
                final ClientTask clientTask = new ClientTask(socketClient,dbServer);
                clientIOWorkers.submit(clientTask);
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        });
    }

    /**
     * Закрывает все, что нужно ¯\_(ツ)_/¯
     */
    @Override
    public void close() {
        System.out.println("Stopping socket connector");
        try {
            serverSocket.close();
            connectionAcceptorExecutor.shutdownNow();
            clientIOWorkers.shutdownNow();
        } catch (IOException exception){
            exception.printStackTrace();
        }
    }


    public static void main(String[] args) throws Exception {
    }

    /**
     * Runnable, описывающий исполнение клиентской команды.
     */
    static class ClientTask implements Runnable, Closeable {

      //  private RespWriter respWriter;
      // private RespReader respReader;
        private Socket clientSocket;
        DatabaseServer databaseServer;
        /**
         * @param client клиентский сокет
         * @param server сервер, на котором исполняется задача
         */
        public ClientTask(Socket client, DatabaseServer server) {
            databaseServer = server;
            clientSocket = client;
            // respReader = new RespReader(client.getInputStream());
            // respWriter = new RespWriter(client.getOutputStream());
        }

        /**
         * Исполняет задачи из одного клиентского сокета, пока клиент не отсоединился или текущий поток не был прерван (interrupted).
         * Для кажной из задач:
         * 1. Читает из сокета команду с помощью {@link CommandReader}
         * 2. Исполняет ее на сервере
         * 3. Записывает результат в сокет с помощью {@link RespWriter}
         */
        @Override
        public void run() {
        //    CommandReader commandReader = new CommandReader(respReader, databaseServer.getEnv());
         //   final RespReader respReader = new RespReader(clientSocket.getInputStream());
            try {
                final RespReader respReader = new RespReader(clientSocket.getInputStream());
                final RespWriter respWriter = new RespWriter(clientSocket.getOutputStream());
                CommandReader commandReader = new CommandReader(respReader, databaseServer.getEnv());
                while (!Thread.currentThread().isInterrupted() && !clientSocket.isClosed()) {
                    if (commandReader.hasNextCommand()) {
                        DatabaseCommand databaseCommand = commandReader.readCommand();
                        respWriter.write(databaseCommand.execute().serialize());
                    } else {
                        commandReader.close();
                        break;
                    }
                }
                commandReader.close();
            } catch (Exception e) {
                e.printStackTrace();
            }


        }

        /**
         * Закрывает клиентский сокет
         */
        @Override
        public void close() {
            try {
                clientSocket.close();
               // respReader.close();
              //  respWriter.close();
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }
    }
}