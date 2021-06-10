package com.itmo.java.basics.console.impl;

import com.itmo.java.basics.console.DatabaseCommand;
import com.itmo.java.basics.console.DatabaseCommandArgPositions;
import com.itmo.java.basics.console.DatabaseCommandResult;
import com.itmo.java.basics.console.ExecutionEnvironment;
import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.logic.Database;
import com.itmo.java.protocol.model.RespObject;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;

/**
 * Команда для создания записи значения
 */
public class SetKeyCommand implements DatabaseCommand {

    private final ExecutionEnvironment environment;
    private final String dbName;
    private final String tbName;
    private final String key;
    private final String value;

    /**
     * Создает команду.
     * <br/>
     * Обратите внимание, что в конструкторе нет логики проверки валидности данных. Не проверяется, можно ли исполнить команду. Только формальные признаки (например, количество переданных значений или ненуловость объектов
     *
     * @param env         env
     * @param commandArgs аргументы для создания (порядок - {@link DatabaseCommandArgPositions}.
     *                    Id команды, имя команды, имя бд, таблицы, ключ, значение
     * @throws IllegalArgumentException если передано неправильное количество аргументов
     */
    public SetKeyCommand(ExecutionEnvironment env, List<RespObject> commandArgs) {
        environment = env;
        value = commandArgs.get(DatabaseCommandArgPositions.VALUE.getPositionIndex()).asString();
        dbName = commandArgs.get(DatabaseCommandArgPositions.DATABASE_NAME.getPositionIndex()).asString();
        tbName = commandArgs.get(DatabaseCommandArgPositions.TABLE_NAME.getPositionIndex()).asString();
        key = commandArgs.get(DatabaseCommandArgPositions.KEY.getPositionIndex()).asString();

    }

    /**
     * Записывает значение
     *
     * @return {@link DatabaseCommandResult#success(byte[])} c предыдущим значением. Например, "previous" или null, если такого не было
     */
    @Override
    public DatabaseCommandResult execute() {
        try {
            Optional<Database> database = environment.getDatabase(dbName);
            if (database.isEmpty()) {
                return DatabaseCommandResult.error("We dont have SetKeyCommand" + dbName);
            }
            Optional<byte[]> previous = database.get().read(tbName, key);
            database.get().write(tbName, key, value.getBytes(StandardCharsets.UTF_8));
            return DatabaseCommandResult.success(previous.orElse(null));
        } catch (DatabaseException e){
            return DatabaseCommandResult.error("Error when try to set value by key");
        }
    }
}
