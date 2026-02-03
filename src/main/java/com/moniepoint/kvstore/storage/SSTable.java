package com.moniepoint.kvstore.storage;

import com.moniepoint.kvstore.index.SSTableIndex;
import com.moniepoint.kvstore.model.Record;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.NavigableMap;

public class SSTable {

    private final Path dataFilePath;
    private final SSTableIndex index;

    public SSTable(Path directory, NavigableMap<String, Record> data) throws IOException {
        Files.createDirectories(directory);

        String fileName = "sst_" + System.currentTimeMillis();
        this.dataFilePath = directory.resolve(fileName + ".dat");
        this.index = new SSTableIndex();

        try (RandomAccessFile randomAccessFile = new RandomAccessFile(this.dataFilePath.toFile(), "rw")) {
            for (Record record : data.values()) {
                Long offset = randomAccessFile.getFilePointer();
                index.getOffsetsMap().put(record.getKey(), offset);
                randomAccessFile.writeUTF(record.getKey().toString());
                randomAccessFile.writeLong(record.getTimestamp());
                randomAccessFile.writeBoolean(record.getTombstone());
                randomAccessFile.writeUTF(record.getValue() == null ? "" : record.getValue().toString());
            }
        }

        try (ObjectOutputStream objectOutputStream = new ObjectOutputStream(Files.newOutputStream(directory.resolve(fileName + ".idx")))) {
            objectOutputStream.writeObject(this.index);
        }
    }

    public Record get(String key) throws IOException {
        Map.Entry<String, Long> entry = index.getOffsetsMap().floorEntry(key);

        if (entry == null || !entry.getKey().equals(key)) {
            return null;
        }

        try (RandomAccessFile randomAccessFile = new RandomAccessFile(this.dataFilePath.toFile(), "r")) {
            randomAccessFile.seek(entry.getValue());
            String currentKey = randomAccessFile.readUTF();
            Long timestamp = randomAccessFile.readLong();
            Boolean tombStone = randomAccessFile.readBoolean();
            String value = randomAccessFile.readUTF();
            return new Record(currentKey, value.isEmpty() ? "" : value, timestamp, tombStone);
        }
    }
}
