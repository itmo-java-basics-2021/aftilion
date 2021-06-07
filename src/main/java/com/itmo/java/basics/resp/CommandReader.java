package com.itmo.java.basics.resp;

import com.itmo.java.basics.console.DatabaseCommand;
import com.itmo.java.basics.console.DatabaseCommandArgPositions;
import com.itmo.java.basics.console.DatabaseCommands;
import com.itmo.java.basics.console.ExecutionEnvironment;
import com.itmo.java.protocol.RespReader;
import com.itmo.java.protocol.model.RespObject;

import java.io.IOException;
import java.util.List;

public class CommandReader implements AutoCloseable {

    private final RespReader respReader;
    private final ExecutionEnvironment executionEnvironment;
    public CommandReader(RespReader reader, ExecutionEnvironment env) {
        respReader = reader;
        executionEnvironment = env;
    }

    /**
     * Есть ли следующая команда в ридере?
     */
    public boolean hasNextCommand() throws IOException {
       return respReader.hasArray();
    }

    /**
     * Считывает комманду с помощью ридера и возвращает ее
     *
     * @throws IllegalArgumentException если нет имени команды и id
     */
    public DatabaseCommand readCommand() throws IOException {
        List<RespObject> comArgs = respReader.readArray().getObjects();
        if(comArgs.size() < 2) {
            throw new IllegalArgumentException("Why we dont have id and command name?");
        }
        return DatabaseCommands.valueOf(comArgs.get(DatabaseCommandArgPositions.COMMAND_NAME.getPositionIndex()).asString())
                .getCommand(executionEnvironment,comArgs);
    }

    @Override
    public void close() throws Exception {
       respReader.close();
    }
}
