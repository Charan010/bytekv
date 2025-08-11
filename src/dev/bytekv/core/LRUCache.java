package dev.bytekv.core;

import java.util.*;
import java.util.concurrent.locks.*;

// cache - 


public class LRUCache<K, V> {
    private final LinkedHashMap<K, V> map;
    private final int capacity;
    private int totalHits;
    private int succesfulHits;
    
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final Lock readLock = lock.readLock();
    private final Lock writeLock = lock.writeLock();

    public LRUCache(int capacity) {

        this.capacity = capacity;
        this.totalHits = 0;
        this.succesfulHits = 0;
        this.map = new LinkedHashMap<>(capacity, 0.75f, true) {
            protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
                return size() > LRUCache.this.capacity;
            }
        };
    }

    public V get(K key) {
        readLock.lock();
        try {

            this.totalHits++;
            if(map.get(key)!=null)
                this.succesfulHits++;

            return map.get(key);

        } finally {
            readLock.unlock();
        }
    }

    public void put(K key, V value) {
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

    public String getStats(){
        String ans = String.valueOf(totalHits) + "," + String.valueOf(succesfulHits) + "," + String.valueOf(capacity);
        return ans;
    }

}
