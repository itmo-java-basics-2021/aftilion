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
 * Команда для чтения данных по ключу
 */
public class GetKeyCommand implements DatabaseCommand {
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
    public GetKeyCommand(ExecutionEnvironment env, List<RespObject> commandArgs) {
        this.environment = env;
        this.dbName = commandArgs.get(DatabaseCommandArgPositions.DATABASE_NAME.getPositionIndex()).asString();
        this.tbName = commandArgs.get(DatabaseCommandArgPositions.TABLE_NAME.getPositionIndex()).asString();
        this.key = commandArgs.get(DatabaseCommandArgPositions.KEY.getPositionIndex()).asString();
    }

    /**
     * Читает значение по ключу
     *
     * @return {@link DatabaseCommandResult#success(byte[])} с прочитанным значением. Например, "previous". Null, если такого нет
     */
    @Override
    public DatabaseCommandResult execute() {
        try {
            Optional<Database> database = environment.getDatabase(dbName);
            if (database.isEmpty()){
                return DatabaseCommandResult.error("");
            }
            Optional<byte[]> value = database.get().read(tbName, key);
            return DatabaseCommandResult.success(value.orElse(null));
        } catch (DatabaseException e){
            return DatabaseCommandResult.error("");
        }
    }
}
