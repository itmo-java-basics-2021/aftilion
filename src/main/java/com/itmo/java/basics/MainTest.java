package com.itmo.java.basics;

import com.itmo.java.basics.config.DatabaseConfig;
import com.itmo.java.basics.console.impl.ExecutionEnvironmentImpl;
import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.initialization.*;
import com.itmo.java.basics.initialization.impl.*;
import com.itmo.java.basics.logic.*;
import com.itmo.java.basics.logic.impl.*;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.Optional;

public class MainTest {
    public static void main(String[] args) throws DatabaseException, IOException {
//        Files.walk(Path.of("db", "anime"))
//                .sorted(Comparator.reverseOrder())
//                .map(Path::toFile)
//                .forEach(File::delete);
        try {
            Database db = DatabaseImpl.create("anime", Path.of("db"));
            db.createTableIfNotExists("naruto");
            db.write("naruto", "key", "value".getBytes(StandardCharsets.UTF_8));
            db.write("naruto", "key", "value2".getBytes(StandardCharsets.UTF_8));
            db.write("naruto", "key", null);
            db.write("naruto", "key1", "value1".getBytes(StandardCharsets.UTF_8));
            db.write("naruto", "key2", "".getBytes(StandardCharsets.UTF_8));
        }
        catch (DatabaseException ex)
        {throw  new DatabaseException(ex);}
// System.out.printf(
// "get = %s expected = %s\n",
// new String(db.read("naruto", "saske").get()),
// "ora"
// );
// db.write("naruto", "saske", null);
// db.delete("naruto", "saske");
// System.out.printf(
// "get = %s expected = %s\n",
// db.read("naruto", "saske").toString(),
// Optional.empty().toString()
// );
// db.write("naruto", "saske", "ora".getBytes(StandardCharsets.UTF_8));
// System.out.printf(
// "get = %s expected = %s\n",
// new String(db.read("naruto", "saske").get()),
// "ora"
// );

//        Initializer initializer =
//                new DatabaseServerInitializer(
//                        new DatabaseInitializer(
//                                new TableInitializer(
//                                        new SegmentInitializer())));
//        var execEnv = new ExecutionEnvironmentImpl(
//                new DatabaseConfig("db")
//        );
//        var context = new InitializationContextImpl(
//                execEnv, null, null, null
//        );
//        initializer.perform(context);
    }
}