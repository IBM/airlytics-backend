package com.ibm.airlytics.consumer.userdb;

import java.util.LinkedHashMap;
import java.util.Map;

public class UserCache extends LinkedHashMap<String, UserBasicRow> {
    private final int maxEntries;

    public UserCache(final int maxEntries) {
        super(maxEntries + 1, 1.0f, true);
        this.maxEntries = maxEntries;
    }

    @Override
    protected boolean removeEldestEntry(final Map.Entry<String, UserBasicRow> eldest) {
        return super.size() > maxEntries;
    }
}
