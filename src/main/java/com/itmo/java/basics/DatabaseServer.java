package com.itmo.java.basics;

import com.itmo.java.basics.console.*;
import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.initialization.impl.DatabaseServerInitializer;
import com.itmo.java.basics.initialization.impl.InitializationContextImpl;
import com.itmo.java.protocol.model.RespArray;
import com.itmo.java.protocol.model.RespObject;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class DatabaseServer {
    private final ExecutorService executorService = Executors.newSingleThreadExecutor();
    private final ExecutionEnvironment environment;

    private DatabaseServer(ExecutionEnvironment env) {
        environment = env;
    }

    /**
     * Конструктор
     *
     * @param env         env для инициализации. Далее работа происходит с заполненным объектом
     * @param initializer готовый чейн инициализации
     * @throws DatabaseException если произошла ошибка инициализации
     */
    public static DatabaseServer initialize(ExecutionEnvironment env, DatabaseServerInitializer initializer) throws DatabaseException {
        initializer.perform(new InitializationContextImpl(env, null, null, null));

        return new DatabaseServer(env);
    }

    public CompletableFuture<DatabaseCommandResult> executeNextCommand(RespArray message) {
        return CompletableFuture.supplyAsync(() -> {
            final List<RespObject> commandArgs = message.getObjects();
            return DatabaseCommands.valueOf(commandArgs
                    .get(DatabaseCommandArgPositions.COMMAND_NAME.getPositionIndex()).asString())
                    .getCommand(environment, commandArgs).execute();
        }, executorService);
    }

    public CompletableFuture<DatabaseCommandResult> executeNextCommand(DatabaseCommand command) {
        return CompletableFuture.supplyAsync(command::execute, executorService);
    }

    public ExecutionEnvironment getEnv() {
        return environment;
    }
}