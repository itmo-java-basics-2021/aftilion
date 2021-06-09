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
 * Команда для создания записи значения
 */
public class SetKeyCommand implements DatabaseCommand {

    private final ExecutionEnvironment environment;
    private final List<RespObject> commandargs;
    private static final int numberOfAgrguments = 6;
    private  final String dbName;
    private  final String tbName;
    private  final String key;
    private final String value;

    /**
     * Создает команду.
     * <br/>
     * Обратите внимание, что в конструкторе нет логики проверки валидности данных. Не проверяется, можно ли исполнить команду. Только формальные признаки (например, количество переданных значений или ненуловость объектов
     *
     * @param env     env
     * @param comArgs аргументы для создания (порядок - {@link DatabaseCommandArgPositions}.
     *                Id команды, имя команды, имя бд, таблицы, ключ, значение
     * @throws IllegalArgumentException если передано неправильное количество аргументов
     */
    public SetKeyCommand(ExecutionEnvironment env, List<RespObject> comArgs) {
        if (comArgs.size() != numberOfAgrguments) {
            throw new IllegalArgumentException("Why " + comArgs.size() + "!= 5 , in CreateTableCommand");
        }
        environment = env;
        commandargs = comArgs;
        dbName = comArgs.get(DatabaseCommandArgPositions.DATABASE_NAME.getPositionIndex()).asString();
        tbName = comArgs.get(DatabaseCommandArgPositions.TABLE_NAME.getPositionIndex()).asString();
        key = comArgs.get(DatabaseCommandArgPositions.KEY.getPositionIndex()).asString();
        value = comArgs.get(DatabaseCommandArgPositions.VALUE.getPositionIndex()).asString();
    }

    /**
     * Записывает значение
     *
     * @return {@link DatabaseCommandResult#success(byte[])} c предыдущим значением. Например, "previous" или null, если такого не было
     */
    @Override
    public DatabaseCommandResult execute() {
        try {

            if (dbName == null) {
                throw new DatabaseException("Why dbname is null?");
            }

            if (tbName == null) {
                throw new DatabaseException("Why tbName is null?");
            }

            if (key == null) {
                throw new DatabaseException("Why key is null?");
            }
            Optional<Database> dataBase = environment.getDatabase(dbName);
            if (dataBase.isEmpty()) {
                return DatabaseCommandResult.error("DB is not found");
            }
            Optional<byte[]> previous = dataBase.get().read(tbName,key);
            dataBase.get().write(tbName, key, value.getBytes(StandardCharsets.UTF_8));
            return DatabaseCommandResult.success(previous.orElse(null));
        } catch (DatabaseException ex) {
            return DatabaseCommandResult.error("Error while setkey");
        }
    }
}
