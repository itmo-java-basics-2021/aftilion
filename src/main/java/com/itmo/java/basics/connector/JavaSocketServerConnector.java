package com.itmo.java.basics.connector;

import com.itmo.java.basics.DatabaseServer;
import com.itmo.java.basics.config.ConfigLoader;
import com.itmo.java.basics.config.DatabaseConfig;
import com.itmo.java.basics.config.DatabaseServerConfig;
import com.itmo.java.basics.config.ServerConfig;
import com.itmo.java.basics.console.DatabaseCommand;
import com.itmo.java.basics.console.DatabaseCommandResult;
import com.itmo.java.basics.console.ExecutionEnvironment;
import com.itmo.java.basics.console.impl.ExecutionEnvironmentImpl;
import com.itmo.java.basics.initialization.impl.DatabaseInitializer;
import com.itmo.java.basics.initialization.impl.DatabaseServerInitializer;
import com.itmo.java.basics.initialization.impl.SegmentInitializer;
import com.itmo.java.basics.initialization.impl.TableInitializer;
import com.itmo.java.basics.resp.CommandReader;
import com.itmo.java.client.command.CreateDatabaseKvsCommand;
import com.itmo.java.client.command.CreateTableKvsCommand;
import com.itmo.java.client.command.GetKvsCommand;
import com.itmo.java.client.command.SetKvsCommand;
import com.itmo.java.client.connection.ConnectionConfig;
import com.itmo.java.client.connection.SocketKvsConnection;
import com.itmo.java.protocol.RespReader;
import com.itmo.java.protocol.RespWriter;
import com.itmo.java.protocol.model.RespObject;

import java.io.Closeable;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.CompletableFuture;
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

    private final ServerSocket serverSocket;
    private final ExecutorService connectionAcceptorExecutor = Executors.newSingleThreadExecutor();

    private final DatabaseServer server;

    /**
     * Стартует сервер. По аналогии с сокетом открывает коннекшн в конструкторе.
     */
    public JavaSocketServerConnector(DatabaseServer databaseServer, ServerConfig config) throws IOException {
        try {
            this.serverSocket = new ServerSocket(config.getPort());
            this.server = databaseServer;
        } catch (IOException ex) {
            throw new IOException(ex);
        }
    }

    /**
     * Начинает слушать заданный порт, начинает аксептить клиентские сокеты. На каждый из них начинает клиентскую таску
     */
    public void start() {
        connectionAcceptorExecutor.submit(() -> {
            while(true) {
                Socket clientSocket = serverSocket.accept();
                clientIOWorkers.submit(new ClientTask(clientSocket,server));
            }
        });
    }
    /**
     * Закрывает все, что нужно ¯\_(ツ)_/¯
     */
    @Override
    public void close() {
        System.out.println("Stopping socket connector");
        connectionAcceptorExecutor.shutdownNow();
        clientIOWorkers.shutdownNow();
        if (serverSocket != null) {
            try {
                serverSocket.close();
            } catch (IOException e) {
                throw new RuntimeException("IOException when try to close connection", e);
            }
        }
    }


    public static void main(String[] args) throws Exception {
        ServerConfig serverConfig = new ConfigLoader().readConfig().getServerConfig();
        DatabaseConfig databaseConfig = new ConfigLoader().readConfig().getDbConfig();
        ExecutionEnvironment env = new ExecutionEnvironmentImpl(databaseConfig);
        DatabaseServerInitializer initializer =
                new DatabaseServerInitializer(
                        new DatabaseInitializer(
                                new TableInitializer(
                                        new SegmentInitializer())));
        DatabaseServer databaseServer = DatabaseServer.initialize(env, initializer);

        JavaSocketServerConnector j = new JavaSocketServerConnector(databaseServer, serverConfig);

        j.start();
        RespObject q;
        try {
            SocketKvsConnection socketKvsConnection = new SocketKvsConnection(new ConnectionConfig(serverConfig.getHost(), serverConfig.getPort()));
        } catch (Exception ex) {
            ex.printStackTrace();
        }
//        try(SocketKvsConnection socketKvsConnection =
//                    new SocketKvsConnection(new ConnectionConfig(serverConfig.getHost(), serverConfig.getPort()))) {
//            socketKvsConnection.send(1, new CreateDatabaseKvsCommand("t1").serialize());
//            socketKvsConnection.send(1, new CreateTableKvsCommand("t1", "da").serialize());
//            socketKvsConnection.send(1, new SetKvsCommand("t1", "da", "key1", "value1").serialize());
//            q = socketKvsConnection.send(1, new GetKvsCommand("t1", "da", "key1").serialize());
//        }
       // System.out.println(q.asString());
        j.close();
    }

    /**
     * Runnable, описывающий исполнение клиентской команды.
     */
    static class ClientTask implements Runnable, Closeable {
        private final Socket client;
        private final DatabaseServer server;
        private final RespWriter respWriter;
        private final RespReader respReader;

        /**
         * @param client клиентский сокет
         * @param server сервер, на котором исполняется задача
         */
        public ClientTask(Socket client, DatabaseServer server) {
            this.client = client;
            this.server = server;
            try {
                this.respWriter = new RespWriter(client.getOutputStream());
                this.respReader = new RespReader(client.getInputStream());
            } catch (IOException e){
                throw new RuntimeException("IOException when open socket streams", e);
            }
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
            CommandReader commandReader = new CommandReader(respReader, server.getEnv());
            try {
                while (!Thread.currentThread().isInterrupted() && !client.isClosed()) {
                    if (commandReader.hasNextCommand()) {
                        DatabaseCommand dbCommand = commandReader.readCommand();
                        respWriter.write(dbCommand.execute().serialize());
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
                respWriter.close();
                client.close();
            } catch (IOException e){
                throw new RuntimeException("When try to close client connection", e);
            }
        }
    }
}
