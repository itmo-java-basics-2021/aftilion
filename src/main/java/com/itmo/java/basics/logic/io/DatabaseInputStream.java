package com.itmo.java.basics.logic.io;

import com.itmo.java.basics.logic.DatabaseRecord;
import com.itmo.java.basics.logic.WritableDatabaseRecord;
import com.itmo.java.basics.logic.impl.SetDatabaseRecord;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Optional;

/**
 * Класс, отвечающий за чтение данных из БД
 */
public class DatabaseInputStream extends DataInputStream {
    private static final int REMOVED_OBJECT_SIZE = -1;

    public DatabaseInputStream(InputStream inputStream) {
        super(inputStream);
    }

     /**
     * Читает следующую запись (см {@link DatabaseOutputStream#write(WritableDatabaseRecord)})
     * @return следующую запись, если она существует. {@link Optional#empty()} - если конец файла достигнут
     */
    public Optional<DatabaseRecord> readDbUnit() throws IOException {

        if (available() <= 4)
        {
           return Optional.empty();
        }
          int keySize = readInt();
        if (available() <= 0)
        {
            return Optional.empty();
        }
            byte[] key = readNBytes(keySize);
        if (available() <= 4)
        {
            return Optional.empty();
        }
           int valueSize = readInt();

        if ((available() <= 0) && (valueSize == REMOVED_OBJECT_SIZE))
        {
            return Optional.empty();
        }

                byte[] value = readNBytes(valueSize);
                SetDatabaseRecord datarecord = new SetDatabaseRecord(key,value);
               // datarecord.key = key;
               // datarecord.value= value;
                return Optional.of(datarecord);
            }
    }

