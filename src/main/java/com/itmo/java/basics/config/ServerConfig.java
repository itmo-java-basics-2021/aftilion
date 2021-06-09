package com.itmo.java.basics.config;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * Какой хост и какой порт будет слушать наш сервер
 */
@Getter
public class ServerConfig {

    public static final String DEFAULT_HOST = "localhost";
    public static final int DEFAULT_PORT = 8080;

    private final String host;
    private final int port;

    public ServerConfig(String host , int port) {
        this.host =  host;
        this.port = port;
    }
}
