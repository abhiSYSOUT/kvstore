package com.moniepoint.kvstore.engine;

import com.moniepoint.kvstore.model.Record;
import com.moniepoint.kvstore.storage.MemTable;
import com.moniepoint.kvstore.storage.SSTable;
import com.moniepoint.kvstore.storage.WriteAheadLog;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class KeyValueStoreEngine {
    private final Long memoryLimit;
    private final MemTable memTable;
    private final List<SSTable> ssTables;
    private final WriteAheadLog writeAheadLog;
    private final Path dataDirectoryPath;

    public KeyValueStoreEngine(Path dataDirecttoryPath, Long memoryLimit) throws IOException {
        this.memoryLimit = memoryLimit;
        this.memTable = new MemTable();
        this.ssTables = new ArrayList<>();
        this.dataDirectoryPath = dataDirecttoryPath;
        this.writeAheadLog = new WriteAheadLog(dataDirecttoryPath.resolve("wal.log"));
        WriteAheadLog.replay(dataDirecttoryPath.resolve("wal.log"), this.memTable);
    }

    public synchronized void put(String key, String value) throws IOException {
        Record record = new Record(key, value, System.currentTimeMillis(), false);
        this.writeAheadLog.append(record);
        this.memTable.put(record);
        overFlowCheck();
    }

    public synchronized void delete(String key) throws IOException {
        Record record = new Record(key, null, System.currentTimeMillis(), true);
        this.writeAheadLog.append(record);
        this.memTable.put(record);
        overFlowCheck();
    }

    public synchronized String read(String key) throws IOException {
        Record record = this.memTable.get(key);

        if (record != null) {
            return record.getTombstone() ? null : record.getValue();
        }

        for (int i = this.ssTables.size() - 1; i >= 0; i--) {
            record = this.ssTables.get(i).get(key);

            if (record != null) {
                return record.getTombstone() ? null : record.getValue();
            }
        }
        return null;
    }

    public synchronized Map<String, String> readKeyRange(String startKey, String endKey) {
        Map<String, String> result = new TreeMap<>();
        this.memTable.range(startKey, endKey).forEach((key, value) -> {
            if (!value.getTombstone()) {
                result.put(key, value.getValue());
            }
        });
        return result;
    }

    private void overFlowCheck() throws IOException {
        if (this.memTable.getSizeBytes() > this.memoryLimit) {
            this.ssTables.add(new SSTable(this.dataDirectoryPath, this.memTable.snapshot()));
            this.memTable.clear();
        }
    }
}
