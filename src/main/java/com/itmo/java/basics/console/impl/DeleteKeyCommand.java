package com.itmo.java.basics.console.impl;

import com.itmo.java.basics.console.DatabaseCommand;
import com.itmo.java.basics.console.DatabaseCommandArgPositions;
import com.itmo.java.basics.console.DatabaseCommandResult;
import com.itmo.java.basics.console.ExecutionEnvironment;
import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.logic.Database;
import com.itmo.java.protocol.model.RespObject;

import java.util.List;
import java.util.Optional;

/**
 * Команда для создания удаления значения по ключу
 */
public class DeleteKeyCommand implements DatabaseCommand {
    private final ExecutionEnvironment environment;
    private final String dbName;
    private final String tbName;
    private final String key;

    /**
     * Создает команду.
     * <br/>
     * Обратите внимание, что в конструкторе нет логики проверки валидности данных. Не проверяется, можно ли исполнить команду. Только формальные признаки (например, количество переданных значений или ненуловость объектов
     *
     * @param env         env
     * @param commandArgs аргументы для создания (порядок - {@link DatabaseCommandArgPositions}.
     *                    Id команды, имя команды, имя бд, таблицы, ключ
     * @throws IllegalArgumentException если передано неправильное количество аргументов
     */
    public DeleteKeyCommand(ExecutionEnvironment env, List<RespObject> commandArgs) {
        this.environment = env;
        this.dbName = commandArgs.get(DatabaseCommandArgPositions.DATABASE_NAME.getPositionIndex()).asString();
        this.tbName = commandArgs.get(DatabaseCommandArgPositions.TABLE_NAME.getPositionIndex()).asString();
        this.key = commandArgs.get(DatabaseCommandArgPositions.KEY.getPositionIndex()).asString();
    }

    /**
     * Удаляет значение по ключу
     *
     * @return {@link DatabaseCommandResult#success(byte[])} с удаленным значением. Например, "previous"
     */
    @Override
    public DatabaseCommandResult execute() {
        try {
            Optional<Database> database = environment.getDatabase(dbName);
            if (database.isEmpty()){
                return DatabaseCommandResult.error("Not found database DeleteKeyCommand " + dbName);
            }
            Optional<byte[]> previous = database.get().read(tbName, key);
            if (previous.isEmpty()){
                return DatabaseCommandResult.error("Value with key DeleteKeyCommand " + key + " in database " + dbName + " not found");
            }
            database.get().delete(tbName, key);
            return DatabaseCommandResult.success(previous.get());
        } catch (DatabaseException e){
            return DatabaseCommandResult.error("DatabaseException when try to delete value by key  DeleteKeyCommand" + key + " in table " + tbName);
        }
    }
}
