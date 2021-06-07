package com.itmo.java.basics.console;

import com.itmo.java.basics.exceptions.DatabaseException;

public interface DatabaseCommand {
    /**
     * Запускает команду.
     *
     * @return Сообщение о выполнении результата команды.
     */
    DatabaseCommandResult execute() ;
}
