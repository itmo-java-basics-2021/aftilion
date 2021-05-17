package com.itmo.java.basics.console.impl;

import com.itmo.java.basics.console.DatabaseCommand;
import com.itmo.java.basics.console.DatabaseCommandArgPositions;
import com.itmo.java.basics.console.DatabaseCommandResult;
import com.itmo.java.basics.console.ExecutionEnvironment;
import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.logic.Database;
import com.itmo.java.basics.logic.DatabaseFactory;
import com.itmo.java.protocol.model.RespObject;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;

/**
 * Команда для создания базы таблицы
 */
public class CreateTableCommand implements DatabaseCommand {

    private final ExecutionEnvironment environment;
    private final  List<RespObject> commandargs;
    private static final int numberOfAgrguments = 4;
    /**
     * Создает команду
     * <br/>
     * Обратите внимание, что в конструкторе нет логики проверки валидности данных. Не проверяется, можно ли исполнить команду. Только формальные признаки (например, количество переданных значений или ненуловость объектов
     *
     * @param env         env
     * @param comArgs аргументы для создания (порядок - {@link DatabaseCommandArgPositions}.
     *                    Id команды, имя команды, имя бд, имя таблицы
     * @throws IllegalArgumentException если передано неправильное количество аргументов
     */
    public CreateTableCommand(ExecutionEnvironment env, List<RespObject> comArgs) {
        environment = env;
        commandargs = comArgs;
        if (comArgs.size() != numberOfAgrguments){
            throw new IllegalArgumentException("Why " + comArgs.size()+"!= 4 , in CreateTableCommand" );
        }
    }

    /**
     * Создает таблицу в нужной бд
     *
     * @return {@link DatabaseCommandResult#success(byte[])} с сообщением о том, что заданная таблица была создана. Например, "Table table1 in database db1 created"
     */
    @Override
    public DatabaseCommandResult execute() {
        try{
            String dbName = commandargs.get(DatabaseCommandArgPositions.DATABASE_NAME.getPositionIndex()).asString();
            if(dbName == null){
                throw new DatabaseException("Why dbname is null?");
            }
            String tbName = commandargs.get(DatabaseCommandArgPositions.TABLE_NAME.getPositionIndex()).asString();
            if(tbName == null){
                throw new DatabaseException("Why tbName is  null?");
            }
            Optional<Database> dataBase = environment.getDatabase(dbName);
            if(dataBase.isEmpty()){
                throw new DatabaseException("We dont have"+ dbName);
            }
            dataBase.get().createTableIfNotExists(tbName);
            return DatabaseCommandResult.success(("Success add " + dbName + tbName).getBytes(StandardCharsets.UTF_8));
        } catch (DatabaseException ex){
            return new FailedDatabaseCommandResult(ex.getMessage());
        }
    }
}
