package dev.bytekv.core;

import java.util.concurrent.Future;

public interface KVStore {
    Future<String> put(String key, String value);
    Future<String> put(String key, String value, long expiryTime);
    Future<String> getexp(String key);
    Future<String> get(String key);
    Future<String> delete(String key);
    Future<String> delete(String key, long expiryTime);
    

    void shutDown();
    void compactLogging(boolean flag);
    void logging(boolean flag);
    void replayLogs();
    void flush();

    static KVStore createDB(){
        return new KeyValue(100 ,500 , "logs/master.log", "logs");

    }
    
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
