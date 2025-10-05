package dev.bytekv.core.storage;

import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;
import java.io.*;

public class MemTable {

    private static final int DEFAULT_FLUSH_THRESHOLD = 1024;
    private static final String TOMBSTONE = "__<deleted>__";

    private final ConcurrentSkipListMap<String, String> buffer = new ConcurrentSkipListMap<>();
    private final SSTManager sstManager;
    private final int flushThreshold;

    public MemTable(SSTManager sstManager) {
        this(sstManager, DEFAULT_FLUSH_THRESHOLD);
    }

    public MemTable(SSTManager sstManager, int flushThreshold) {
        this.sstManager = sstManager;
        this.flushThreshold = flushThreshold;
    }

    public void put(String key, String value) throws IOException {
        buffer.put(key, value);
        if (buffer.size() >= flushThreshold)
            swapAndFlush();
    }

    public String delete(String key) throws IOException {
        if (!buffer.containsKey(key))
            return "NO KEY FOUND";

        buffer.put(key, TOMBSTONE);
        if (buffer.size() >= flushThreshold)
            swapAndFlush();

        return "OK!";
    }

    public String get(String key) throws IOException {
        String val = buffer.get(key);
        if (TOMBSTONE.equals(val)) return null;
        if (val != null) return val;

        for (SSTable table : sstManager.getAllSSTables()) {
            val = table.get(key);
            if (TOMBSTONE.equals(val)) return null;
            if (val != null) return val;
        }
        return null;
    }

    private void swapAndFlush() {
        TreeMap<String, String> snapShot;
        synchronized (this) {
            if (buffer.isEmpty())
                return;
            snapShot = new TreeMap<>(buffer);
            buffer.clear();
        }
        new Thread(() -> {
            try {
                sstManager.flushToSSTable(snapShot);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }).start();
    }

    public synchronized void flush() throws IOException {
        if (buffer.isEmpty()) 
            return;
        TreeMap<String, String> snapShot = new TreeMap<>(buffer);
        buffer.clear();
        sstManager.flushToSSTable(snapShot);
    }
}
