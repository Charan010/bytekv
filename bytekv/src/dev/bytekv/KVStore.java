package dev.bytekv;

public interface KVStore {
    void put(String key, String value);
    String get(String key);
    void delete(String key);
    void getAll();
    void shutDown();
    void addCompactLogging();
}
