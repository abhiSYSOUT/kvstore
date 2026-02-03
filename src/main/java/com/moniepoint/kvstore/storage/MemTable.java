package com.moniepoint.kvstore.storage;

import com.moniepoint.kvstore.model.Record;

import java.util.NavigableMap;
import java.util.TreeMap;

public class MemTable {

    private final NavigableMap<String, Record> map = new TreeMap<>();
    private Long sizeBytes = 0L;
    private static MemTable instance;

    public void put(Record record) {
        this.map.put(record.getKey(), record);
        this.sizeBytes += record.getKey().length() + ( record.getValue() == null ? 0 : record.getValue().length());
    }

    public Record get(String key) {
        return this.map.get(key);
    }

    public NavigableMap<String, Record> snapshot() {
        return new TreeMap<>(this.map);
    }

    public void clear() {
        this.map.clear();
        this.sizeBytes = 0L;
    }

    public Long getSizeBytes() {
        return this.sizeBytes;
    }

    public NavigableMap<String, Record> range(String start, String end) {
        return this.map.subMap(start, true, end, true);
    }
}
