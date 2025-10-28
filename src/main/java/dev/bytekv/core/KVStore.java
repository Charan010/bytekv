package dev.bytekv.core;

import java.util.concurrent.CompletableFuture;

public interface KVStore {
    CompletableFuture<String> put(String key, String value);
    CompletableFuture<String> put(String key, String value, long expiryTime);
    CompletableFuture<String> getTTL(String key);
    CompletableFuture<String> get(String key);
    CompletableFuture<String> delete(String key);
    void forceFlush();

    enum ETIME {
        SECONDS(1000),
        MINUTES(60 * 1000),
        HOURS(60 * 60 * 1000),
        DAYS(24 * 60 * 60 * 1000);

        private final long millis;

        ETIME(long millis) {
            this.millis = millis;
        }

        public long toMillis(long amount) {
            return millis * amount;
        }
    }
}
