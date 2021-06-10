package com.itmo.java.basics.console.impl;

import com.itmo.java.basics.console.DatabaseCommand;
import com.itmo.java.basics.console.DatabaseCommandArgPositions;
import com.itmo.java.basics.console.DatabaseCommandResult;
import com.itmo.java.basics.console.ExecutionEnvironment;
import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.logic.Database;
import com.itmo.java.protocol.model.RespObject;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;

/**
 * Команда для создания удаления значения по ключу
 */
public class DeleteKeyCommand implements DatabaseCommand {

    private final ExecutionEnvironment environment;
    private final List<RespObject> commandargs;
    private static final int numberOfAgrguments = 5;
    private final String tbName;
    private final String key;
    private final String dbName;


    /**
     * Создает команду.
     * <br/>
     * Обратите внимание, что в конструкторе нет логики проверки валидности данных. Не проверяется, можно ли исполнить команду. Только формальные признаки (например, количество переданных значений или ненуловость объектов
     *
     * @param env     env
     * @param comArgs аргументы для создания (порядок - {@link DatabaseCommandArgPositions}.
     *                Id команды, имя команды, имя бд, таблицы, ключ
     * @throws IllegalArgumentException если передано неправильное количество аргументов
     */
    public DeleteKeyCommand(ExecutionEnvironment env, List<RespObject> comArgs)  {

        if (comArgs.size() != numberOfAgrguments) {
            throw new IllegalArgumentException("Why " + comArgs.size() + "!= 5 , in CreateTableCommand");
        }
        environment = env;
        commandargs = comArgs;
        dbName = commandargs.get(DatabaseCommandArgPositions.DATABASE_NAME.getPositionIndex()).asString();
        tbName = commandargs.get(DatabaseCommandArgPositions.TABLE_NAME.getPositionIndex()).asString();
        key = commandargs.get(DatabaseCommandArgPositions.KEY.getPositionIndex()).asString();
    }

    /**
     * Удаляет значение по ключу
     *
     * @return {@link DatabaseCommandResult#success(byte[])} с удаленным значением. Например, "previous"
     */
    @Override
    public DatabaseCommandResult execute() {
        try {
//            String dbName = commandargs.get(DatabaseCommandArgPositions.DATABASE_NAME.getPositionIndex()).asString();
            if (dbName == null) {
                throw new DatabaseException("Why dbname is null?");
            }
//            String tbName = commandargs.get(DatabaseCommandArgPositions.TABLE_NAME.getPositionIndex()).asString();
            if (tbName == null) {
                throw new DatabaseException("Why tbName is null?");
            }
        //    String key = commandargs.get(DatabaseCommandArgPositions.KEY.getPositionIndex()).asString();
            if (key == null) {
                throw new DatabaseException("Why key is null?");
            }
            Optional<Database> dataBase = environment.getDatabase(dbName);
            if (dataBase.isEmpty()) {
                throw new DatabaseException("We dont have" + dbName);
            }
            Optional<byte[]> value = dataBase.get().read(tbName,key);
            if (value.isEmpty()) {
                return DatabaseCommandResult.error("Error deleting");
            }
            dataBase.get().delete(tbName, key);
            return DatabaseCommandResult.success(value.get());
        } catch (DatabaseException ex) {
            return DatabaseCommandResult.error("Error in exucute ");
        }
    }
}