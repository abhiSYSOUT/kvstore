package com.moniepoint.kvstore.index;

import java.io.Serializable;
import java.util.TreeMap;

public class SSTableIndex implements Serializable {

    private final TreeMap<String, Long> offsetsMap = new TreeMap<>();

    public TreeMap<String, Long> getOffsetsMap() {
        return offsetsMap;
    }
}
