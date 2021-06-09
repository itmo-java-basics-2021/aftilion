package com.itmo.java.basics.config;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Класс, отвечающий за подгрузку данных из конфигурационного файла формата .properties
 */
public class ConfigLoader {

    private final String DEAFULT_PROPERTY = "server.properties";
    Properties configFileProp = new Properties();
    private String workingPath;
    private String host;
    private int port;
    private InputStream inputStream;

    /**
     * По умолчанию читает из server.properties
     */
    public ConfigLoader() {
        inputStream = getClass().getClassLoader().getResourceAsStream(DEAFULT_PROPERTY);
        try {
            if (inputStream == null) {
               inputStream = new FileInputStream(DEAFULT_PROPERTY);
            }
        } catch (IOException e) {
           inputStream = null;
        }
    }

    /**
     * @param name Имя конфикурационного файла, откуда читать
     */
    public ConfigLoader(String name) {
        inputStream = getClass().getClassLoader().getResourceAsStream(name);
        try {
            if (inputStream == null) {
                inputStream = new FileInputStream(name);
            }
        } catch (IOException e) {
            inputStream = null;
        }
    }

    /**
     * Считывает конфиг из указанного в конструкторе файла.
     * Если не удалось считать из заданного файла, или какого-то конкретно значения не оказалось,
     * то используют дефолтные значения из {@link DatabaseConfig} и {@link ServerConfig}
     * <br/>
     * Читаются: "kvs.workingPath", "kvs.host", "kvs.port" (но в конфигурационном файле допустимы и другие проперти)
     */
    public DatabaseServerConfig readConfig() {
        Properties properties = new Properties();
        try {
            if (inputStream == null) {
                throw new IOException("Config file not found");
            }
            properties.load(inputStream);
            String workingPath = properties.getProperty("kvs.workingPath");
            String host = properties.getProperty("kvs.host");
            String port = properties.getProperty("kvs.port");
            DatabaseConfig databaseConfig;
            ServerConfig serverConfig;
            if (workingPath == null) {
                databaseConfig = new DatabaseConfig();
            } else {
                databaseConfig = new DatabaseConfig(workingPath);
            }
            if (host == null){
                host = ServerConfig.DEFAULT_HOST;
            }
            try {
                int portConfig = Integer.parseInt(port);
                serverConfig = new ServerConfig(host,portConfig);
            } catch (NumberFormatException e) {
                serverConfig = new ServerConfig(host, ServerConfig.DEFAULT_PORT);
            }
            return DatabaseServerConfig.builder().dbConfig(databaseConfig)
                    .serverConfig(serverConfig).build();
        } catch (IOException e) {
            return DatabaseServerConfig.builder()
                    .dbConfig(new DatabaseConfig()).serverConfig(new ServerConfig(ServerConfig.DEFAULT_HOST, ServerConfig.DEFAULT_PORT))
                    .build();
        }
    }
}

