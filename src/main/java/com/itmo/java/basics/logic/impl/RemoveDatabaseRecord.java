package com.itmo.java.basics.logic.impl;

import com.itmo.java.basics.logic.WritableDatabaseRecord;

/**
 * Запись в БД, означающая удаление значения по ключу
 */
public class RemoveDatabaseRecord implements WritableDatabaseRecord {
    private byte[] key;
    private byte[] value;

    public RemoveDatabaseRecord(byte[] key) {
        this.key = key;
    }

    @Override
    public byte[] getKey() {
        return key;
    }

    @Override
    public byte[] getValue() {
        return null;
    }

    @Override
    public long size() {
        return getKeySize() + getValueSize() + 4 + 4;
    }

    @Override
    public boolean isValuePresented() { return false; }

    @Override
    public int getKeySize() {
        return key.length;
    }

    @Override
    public int getValueSize() {
        return -1;
    }
}
