package dev.bytekv.core;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.locks.*;

/**
 * Simple thread-safe LRU cache using LinkedHashMap with read-write locking.
 * Evicts least recently used entry when capacity is exceeded.
 */
public class LRUCache {
    private final LinkedHashMap<String, String> map;
    private final int capacity;

    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final Lock readLock = lock.readLock();
    private final Lock writeLock = lock.writeLock();

    public LRUCache(int capacity) {
        this.capacity = capacity;
        this.map = new LinkedHashMap<>(16, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<String, String> eldest) {
                return size() > LRUCache.this.capacity;
            }
        };
    }

    public String get(String key) {
        readLock.lock();
        try {
            return map.get(key);
        } finally {
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

    public void delete(String key){
        try{
            writeLock.lock();
            map.remove(key);
        
        }finally{
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
