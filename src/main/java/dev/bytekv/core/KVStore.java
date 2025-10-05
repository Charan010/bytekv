package dev.bytekv.core;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.concurrent.Future;

public interface KVStore {
    Future<String> put(String key, String value);
    Future<String> put(String key, String value, long expiryTime);
    Future<String> getTTL(String key);
    Future<String> get(String key);
    Future<String> delete(String key);
    
    void shutDown();
    void compactLogging(boolean flag);
    void replayLogs();
    void flush();

    static KVStore createDB(int threadPoolSize, int blockingQueueSize, String logFolder, int memTableLimit) throws IOException{
        String logFilePath = Paths.get(logFolder, "master.log").toString();
        try{
            return new KeyValue(threadPoolSize ,blockingQueueSize ,logFilePath, logFolder, memTableLimit);
        }catch(IOException e){
            throw new IOException(e);
        }
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
