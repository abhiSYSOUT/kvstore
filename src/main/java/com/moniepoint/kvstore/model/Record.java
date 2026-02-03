package com.moniepoint.kvstore.model;

import java.io.Serializable;

public class Record implements Serializable {

    private final String key;
    private final String value;
    private final Long timestamp;
    private final Boolean tombstone;

    public Record(String key, String value, Long timestamp, Boolean tombstone) {
        this.key = key;
        this.value = value;
        this.timestamp = timestamp;
        this.tombstone =  tombstone;
    }

    public String getKey() {
        return key;
    }

    public String getValue() {
        return value;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public Boolean getTombstone() {
        return tombstone;
    }
}
