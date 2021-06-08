package com.itmo.java.basics.connector;

import com.itmo.java.basics.DatabaseServer;
import com.itmo.java.basics.config.ServerConfig;
import com.itmo.java.basics.console.DatabaseCommand;
import com.itmo.java.basics.resp.CommandReader;
import com.itmo.java.protocol.RespReader;
import com.itmo.java.protocol.RespWriter;
import com.itmo.java.protocol.model.RespObject;

import java.io.Closeable;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
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

    private DatabaseServer dbServer;

    /**
     * Стартует сервер. По аналогии с сокетом открывает коннекшн в конструкторе.
     */
    public JavaSocketServerConnector(DatabaseServer databaseServer, ServerConfig config) throws IOException {
        serverSocket = new ServerSocket(config.getPort());
        dbServer = databaseServer;
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
            connectionAcceptorExecutor.shutdownNow();
            clientIOWorkers.shutdownNow();
        } catch (IOException exception){
            exception.printStackTrace();
        }
    }


    public static void main(String[] args) throws Exception {
/*DatabaseConfig cfg = new DatabaseConfig("C:\\Users\\odmen\\Documents\\testdata");
ExecutionEnvironment env = new ExecutionEnvironmentImpl(cfg);
DatabaseInitializationContext dbContext = null;
TableInitializationContext tableContext = null;
SegmentInitializationContext sgmContext = null;

InitializationContextImpl context = new InitializationContextImpl(env, null, null, null);
Initializer init = new DatabaseServerInitializer(
new DatabaseInitializer(
new TableInitializer(
new SegmentInitializer())));

DatabaseServer dbs = DatabaseServer.initialize(env, (DatabaseServerInitializer) init);
ServerConfig serverConfig = new ServerConfig("localhost", 8080);
JavaSocketServerConnector javaConnector = new JavaSocketServerConnector(dbs,serverConfig);
javaConnector.start();

javaConnector.close();*/
        ByteBuffer bb = ByteBuffer.allocate(11);
        RespReader respReader = new RespReader(new FileInputStream("C:\\Users\\odmen\\Documents\\testdata\\respreader.txt"));
        RespObject respObject = respReader.readBulkString();
        RespObject respObject1 = respObject;
    }

    /**
     * Runnable, описывающий исполнение клиентской команды.
     */
    static class ClientTask implements Runnable, Closeable {

        private RespWriter respWriter;
        private

        RespReader respReader;
        private Socket client;
        DatabaseServer databaseServer;
        /**
         * @param client клиентский сокет
         * @param server сервер, на котором исполняется задача
         */
        public ClientTask(Socket client, DatabaseServer server) {
            try {
                databaseServer = server;
                this.client = client;
                respReader = new RespReader(client.getInputStream());
                respWriter = new RespWriter(client.getOutputStream());
            } catch (IOException e) {
                e.printStackTrace();
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
            CommandReader commandReader = new CommandReader(respReader, databaseServer.getEnv());
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
                client.close();
                respReader.close();
                respWriter.close();
            } catch (IOException exception) {
                exception.printStackTrace();
            }
        }
    }
}