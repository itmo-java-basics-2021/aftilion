package com.itmo.java.basics.config;

import java.io.*;
import java.net.URL;
import java.util.Properties;

/**
 * Класс, отвечающий за подгрузку данных из конфигурационного файла формата .properties
 */
public class ConfigLoader {
    private final String fileName;

    /**
     * По умолчанию читает из server.properties
     */
    public ConfigLoader() {
        fileName = "server.properties";
    }

    /**
     * @param name Имя конфикурационного файла, откуда читать
     */
    public ConfigLoader(String name) { fileName = name; }

    /**
     * Считывает конфиг из указанного в конструкторе файла.
     * Если не удалось считать из заданного файла, или какого-то конкретно значения не оказалось,
     * то используют дефолтные значения из {@link DatabaseConfig} и {@link ServerConfig}
     * <br/>
     * Читаются: "kvs.workingPath", "kvs.host", "kvs.port" (но в конфигурационном файле допустимы и другие проперти)
     */
    public DatabaseServerConfig readConfig() {
        final Properties properties = new Properties();
        final URL resource = this.getClass().getClassLoader().getResource(fileName);
        final String filepath = resource == null ? fileName : resource.getPath();

        try (final InputStream stream = new FileInputStream(filepath)) {
            properties.load(stream);

            final String workingPath = properties.getProperty("kvs.workingPath", DatabaseConfig.DEFAULT_WORKING_PATH);
            final String host = properties.getProperty("kvs.host", ServerConfig.DEFAULT_HOST);
            final int port = Integer.parseInt(properties.getProperty("kvs.port", String.valueOf(ServerConfig.DEFAULT_PORT)));

            return new DatabaseServerConfig(new ServerConfig(host, port), new DatabaseConfig(workingPath));
        } catch (IOException exception){
            return new DatabaseServerConfig(
                    new ServerConfig(ServerConfig.DEFAULT_HOST, ServerConfig.DEFAULT_PORT),
                    new DatabaseConfig()
            );
        }
    }
}
