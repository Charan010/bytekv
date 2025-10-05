package dev.bytekv.core;

import java.util.*;
import java.util.concurrent.locks.*;

/*
     * The main tradeoff that we make in log structured tree(LSM) trees are read efficiency. To get through this,
     we can use some kind of cache which stores hot or recent data and can be retrieved faster.

    * I'm using least recently used eviction strategy to make sure cache eviction is fair and hot data is stored much more efficiently.

*/

public class LRUCache{
    private final LinkedHashMap<String, String> map;
    private final int capacity;
    
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final Lock readLock = lock.readLock();
    private final Lock writeLock = lock.writeLock();

    public LRUCache(int capacity) {
        this.capacity = capacity;
        this.map = new LinkedHashMap<>(capacity, 0.75f, true) {
            protected boolean removeEldestEntry(Map.Entry<String, String> eldest) {
                return size() > LRUCache.this.capacity;
            }
        };
    }

    public String get(String key) {
        readLock.lock();
        try {
            return map.get(key);
        }finally {
            readLock.unlock();
        }
    }

    public void put(String key, String value) {
        writeLock.lock();
        try {
            map.put(key, value);
        } finally {
            writeLock.unlock();
        }
    }

    public int size() {
        readLock.lock();
        try {
            return map.size();
        } finally {
            readLock.unlock();
        }
    }

    public void clear() {
        writeLock.lock();
        try {
            map.clear();
        } finally {
            writeLock.unlock();
        }
    }
}
