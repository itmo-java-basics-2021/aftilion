package com.itmo.java.basics.console.impl;

import com.itmo.java.basics.console.DatabaseCommand;
import com.itmo.java.basics.console.DatabaseCommandArgPositions;
import com.itmo.java.basics.console.DatabaseCommandResult;
import com.itmo.java.basics.console.ExecutionEnvironment;
import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.logic.DatabaseFactory;
import com.itmo.java.protocol.model.RespObject;

import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * Команда для создания базы данных
 */
public class CreateDatabaseCommand implements DatabaseCommand {

    private final ExecutionEnvironment environment;
    private final DatabaseFactory dbfactory;
    private final List<RespObject> commandargs;
    private static final int numberOfAgrguments = 3;

    /**
     * Создает команду.
     * <br/>
     * Обратите внимание, что в конструкторе нет логики проверки валидности данных. Не проверяется, можно ли исполнить команду. Только формальные признаки (например, количество переданных значений или ненуловость объектов
     *
     * @param env     env
     * @param factory функция создания базы данных (пример: DatabaseImpl::create)
     * @param comArgs аргументы для создания (порядок - {@link DatabaseCommandArgPositions}.
     *                Id команды, имя команды, имя создаваемой бд
     * @throws IllegalArgumentException если передано неправильное количество аргументов
     */
    public CreateDatabaseCommand(ExecutionEnvironment env, DatabaseFactory factory, List<RespObject> comArgs) {
        if (comArgs.size() != numberOfAgrguments) {
            throw new IllegalArgumentException("Why " + comArgs.size() + "!= 3 ,  in CreateDataBaseCommand");
        }
        environment = env;
        dbfactory = factory;
        commandargs = comArgs;
    }

    /**
     * Создает бд в нужном env
     *
     * @return {@link DatabaseCommandResult#success(byte[])} с сообщением о том, что заданная база была создана. Например, "Database db1 created"
     */
    @Override
    public DatabaseCommandResult execute() {
        try {
            String dbName = commandargs.get(DatabaseCommandArgPositions.DATABASE_NAME.getPositionIndex()).asString();
            if (dbName == null) {
                throw new DatabaseException("Why dbname is null? ");
            }
            environment.addDatabase(dbfactory.createNonExistent(dbName, environment.getWorkingPath()));
            return DatabaseCommandResult.success(("Success add " + dbName).getBytes(StandardCharsets.UTF_8));
        } catch (DatabaseException ex) {
            return new FailedDatabaseCommandResult(ex.getMessage());
        }
    }
}
