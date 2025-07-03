package dev.bytekv.core;

import java.util.concurrent.Future;

public interface KVStore {
    Future<Boolean> put(String key, String value);
    Future<Boolean> put(String key, String value, long expiryTime);
    Future<String> get(String key);
    Future<Boolean> delete(String key);
    Future<Boolean> delete(String key, long expiryTime);
    void shutDown();
    void compactLogging(boolean flag);
    void replayLogs();

    static KVStore create(int threadPool, int queueSize, String logFilePath, String logPath) {
        return new KeyValue(threadPool, queueSize, logFilePath, logPath);
    }

    enum Time {
        SECONDS(1000),
        MINUTES(60 * 1000),
        HOURS(60 * 60 * 1000),
        DAYS(24 * 60 * 60 * 1000);

        private final long millis;

        Time(long millis) {
            this.millis = millis;
        }

        public long toMillis(long amount) {
            return millis * amount;
        }
    }
}
