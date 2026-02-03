package com.moniepoint.kvstore.engine;

import com.moniepoint.kvstore.storage.MemTable;
import com.moniepoint.kvstore.storage.SSTable;
import com.moniepoint.kvstore.storage.WriteAheadLog;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

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
}
