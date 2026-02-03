package com.moniepoint.kvstore.storage;

import com.moniepoint.kvstore.model.Record;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public class WriteAheadLog {

    private final Path path;
    private final BufferedWriter writer;

    public WriteAheadLog(Path filePath) throws IOException {
        this.path = filePath;

        Files.createDirectories(filePath.getParent());
        this.writer = Files.newBufferedWriter(this.path, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
    }

    public synchronized void append(Record record) throws IOException {
        this.writer.write(serialize(record));
        this.writer.newLine();
        this.writer.flush();
    }

    public void close() throws IOException {
        this.writer.close();
    }

    public static void replay(Path filePath, MemTable memTable) throws IOException {
        if (Files.notExists(filePath)) {
            return;
        }

        try (BufferedReader reader = Files.newBufferedReader(filePath)) {
            String line;
            while ((line = reader.readLine()) != null) {
                Record record = deserialize(line);
                memTable.put(record);
            }
        }
    }

    private static String serialize(Record record) {
        return record.getTimestamp() + "|" + record.getTombstone() + "|" + record.getKey().toString() + "|" + (record.getValue() == null ? "" : record.getValue().toString());
    }

    private static Record deserialize(String recordString) {
        String[] recordData = recordString.split("\\|", 4);
        return new Record(recordData[2], recordData[3].isEmpty() ? null : recordData[2], Long.parseLong(recordData[0]), Boolean.parseBoolean(recordData[1]));
    }
}
