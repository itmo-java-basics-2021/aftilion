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
            dbServer = databaseServer;
            serverSocket = new ServerSocket(config.getPort());
        } catch (IOException ex) {
            throw new IOException("JavaSocketServerConnector error constructor",ex);
        }
    }
 
     /**
     * Начинает слушать заданный порт, начинает аксептить клиентские сокеты. На каждый из них начинает клиентскую таску
     */
     public void start() {
         connectionAcceptorExecutor.submit(() -> {
             try {
                 Socket socket = serverSocket.accept();
                 clientIOWorkers.submit(() -> {
                     ClientTask clientTask = new ClientTask(socket, dbServer);
                     clientTask.run();
                 });
             } catch (IOException exception) {
                 exception.printStackTrace();
             } finally {
                 close();
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
            connectionAcceptorExecutor.shutdown();
            clientIOWorkers.shutdown();
        } catch (IOException exception){
            exception.printStackTrace();
        }
    }


    public static void main(String[] args) throws Exception {
        // можнно запускать прямо здесь
    }

    /**
     * Runnable, описывающий исполнение клиентской команды.
     */
    static class ClientTask implements Runnable, Closeable {
//        private RespWriter respWriter;
//        private RespReader respReader;
//        private Socket clientSocket;
//        DatabaseServer databaseServer;
        /**
         * @param client клиентский сокет
         * @param server сервер, на котором исполняется задача
         */
        public ClientTask(Socket client, DatabaseServer server) {
//            try {
//                databaseServer = server;
//                clientSocket = client;
//                respReader = new RespReader(client.getInputStream());
//                respWriter = new RespWriter(client.getOutputStream());
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
        }

        /**
         * Исполняет задачи из одного клиентского сокета, пока клиент не отсоединился или текущий поток не был прерван (interrupted).
         * Для кажной из задач:
         * 1. Читает из сокета команду с помощью {@link CommandReader}
         * 2. Исполняет ее на сервере
         * 3. Записывает результат в сокет с помощью {@link RespWriter}
         */
        @SneakyThrows
        @Override
        public void run() {
//            CommandReader commandReader = new CommandReader(respReader, databaseServer.getEnv());
//            while (commandReader.hasNextCommand()) {
//                try {
//                    DatabaseCommand dbCommand = commandReader.readCommand();
//                    respWriter.write(dbCommand.execute().serialize());
//                } catch (IOException e) {
//                    e.printStackTrace();
//                }
//            }
        }

        /**
         * Закрывает клиентский сокет
         */
        @Override
        public void close() {
//            try {
//                clientSocket.close();
//                respReader.close();
//                respWriter.close();
//            } catch (IOException ex) {
//               throw new RuntimeException("Error while cloasing socket client" , ex);
//            }
        }
    }
}
