package com.itmo.java.basics.config;

import java.util.Properties;

/**
 * Класс, отвечающий за подгрузку данных из конфигурационного файла формата .properties
 */
public class ConfigLoader {
    /**
     * По умолчанию читает из server.properties
     */
    private final String configFileName;
    private static final String AUTO_NAME = "server.properties";
    private Properties properties = new Properties();
    public ConfigLoader() {
        configFileName = AUTO_NAME;
    }

    /**
     * @param name Имя конфикурационного файла, откуда читать
     */
    public ConfigLoader(String name) {
        configFileName = name;
    }

    /**
     * Считывает конфиг из указанного в конструкторе файла.
     * Если не удалось считать из заданного файла, или какого-то конкретно значения не оказалось,
     * то используют дефолтные значения из {@link DatabaseConfig} и {@link ServerConfig}
     * <br/>
     * Читаются: "kvs.workingPath", "kvs.host", "kvs.port" (но в конфигурационном файле допустимы и другие проперти)
     */
    public DatabaseServerConfig readConfig() {
        String workingPath = properties.getProperty("kvs.workingPath");
        if (workingPath == null){
            workingPath = DatabaseConfig.DEFAULT_WORKING_PATH;
        }
        String host = properties.getProperty("kvs.host");
        if (host == null) {
            host = ServerConfig.DEFAULT_HOST;
        }
        int port;
        if (properties.getProperty("kvs.port") == null){
            port = ServerConfig.DEFAULT_PORT;
        }else {
            port = Integer.parseInt(properties.getProperty("kvs.port"));
        }
        return new DatabaseServerConfig(new ServerConfig(host, port), new DatabaseConfig(workingPath));
    }
}
