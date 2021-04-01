package com.itmo.java.basics;

import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.logic.Database;
import com.itmo.java.basics.logic.impl.DatabaseImpl;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.nio.charset.StandardCharsets;
import java.util.Optional;

public class MainTest {
    public static void main(String[] args){
        try {
            Files.walk(Path.of("db2")).sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(File::delete);
            Database db1 = DatabaseImpl.create("db2", Path.of("."));
            db1.createTableIfNotExists("table1");

            db1.write("table1", "1", "user".getBytes(StandardCharsets.UTF_8));
            System.out.println(new String(db1.read("table1", "1").get()));
            db1.delete("table1", "1");
            System.out.println(new String(db1.read("table1", "1").toString()));

            db1.write("table1", "1", null);

            System.out.println(new String(db1.read("table1", "1").toString()));
        }catch(DatabaseException | IOException exception){
            System.out.print("Error appeared with message : " + exception.getMessage());
        }
    }
}