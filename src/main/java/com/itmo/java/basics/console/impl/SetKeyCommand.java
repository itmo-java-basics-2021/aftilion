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

    private final ExecutionEnvironment enviroment;
    private final List<RespObject> commandargs;
    private static final int numberOfAgrguments = 6;

    /**
     * Создает команду.
     * <br/>
     * Обратите внимание, что в конструкторе нет логики проверки валидности данных. Не проверяется, можно ли исполнить команду. Только формальные признаки (например, количество переданных значений или ненуловость объектов
     *
     * @param env         env
     * @param comArgs аргументы для создания (порядок - {@link DatabaseCommandArgPositions}.
     *                    Id команды, имя команды, имя бд, таблицы, ключ, значение
     * @throws IllegalArgumentException если передано неправильное количество аргументов
     */
    public SetKeyCommand(ExecutionEnvironment env, List<RespObject> comArgs) {
        this.enviroment = env;
        this.commandargs = comArgs;
        if (comArgs.size() != numberOfAgrguments) {
            throw new IllegalArgumentException(String.format("Wrong number of arguments - you need %d but given %d"));
        }
    }

    /**
     * Записывает значение
     *
     * @return {@link DatabaseCommandResult#success(byte[])} c предыдущим значением. Например, "previous" или null, если такого не было
     */
    @Override
    public DatabaseCommandResult execute() {
        try {
            String dbName = commandargs.get(DatabaseCommandArgPositions.DATABASE_NAME.getPositionIndex()).asString();
            String tbName = commandargs.get(DatabaseCommandArgPositions.TABLE_NAME.getPositionIndex()).asString();
            String key = commandargs.get(DatabaseCommandArgPositions.KEY.getPositionIndex()).asString();
            byte[] value = commandargs.get(DatabaseCommandArgPositions.VALUE.getPositionIndex()).asString().getBytes(StandardCharsets.UTF_8);
            Optional<Database> database = enviroment.getDatabase(dbName);
            if (database.isEmpty()) {
                throw new DatabaseException(String.format("There is no database with name %s", dbName));
            }
            database.get().write(tbName, key, value);
            return DatabaseCommandResult.success(String.format("Key %s was successfully added in table %s in database %s", key, tbName, dbName)
                    .getBytes(StandardCharsets.UTF_8));

        } catch (DatabaseException e) {
            return new FailedDatabaseCommandResult(e.getMessage());
        }
    }
}