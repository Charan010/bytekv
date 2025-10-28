package dev.bytekv.core;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

public class KeyValue {

    private final Coordinator cood;

    public KeyValue(String logPath, int memTableLimit) throws IOException {
        try {
            cood = new Coordinator(logPath, memTableLimit);
        } catch (IOException e) {
            throw new IOException(e);
        }
    }

    public CompletableFuture<String> put(String key, String value) {
        if (key == null) return CompletableFuture.completedFuture("ERROR: null key");

        return cood.put(key, value); 
    }

    public CompletableFuture<String> put(String key, String value, long ms) {
        if (key == null) return CompletableFuture.completedFuture("ERROR: null key");

        long expiryTime = System.currentTimeMillis() + ms;
        return cood.put(key, value, expiryTime); 
    }

    public CompletableFuture<String> get(String key) {
        if (key == null) 
            return CompletableFuture.completedFuture("ERROR: null key");
        return cood.get(key); 
    }

    public CompletableFuture<String> getTTL(String key) {
        if (key == null) return CompletableFuture.completedFuture("ERROR: null key");

        return cood.getTTL(key);
    }

    public CompletableFuture<String> delete(String key) {
        if (key == null) return CompletableFuture.completedFuture("ERROR: null key");

        return cood.delete(key);
    }

    public void forceFlush() {
        cood.forceFlush();
    }

    public void shutDown() throws InterruptedException {
        cood.shutDown();
    }
}
