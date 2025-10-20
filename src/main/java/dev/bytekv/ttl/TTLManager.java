package dev.bytekv.ttl;

import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.*;
import dev.bytekv.core.StoreEntry;

/*
    TTLManager responsibilities:
    - Runs periodically
    - Samples random keys and checks for expiry
    - If enough expired in sample, triggers full scan
    - Lazy TTL eviction for memory efficiency
*/

public class TTLManager implements Runnable {

    private volatile boolean running = true;
    private final CountDownLatch shutdownLatch;
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    private final Random random = ThreadLocalRandom.current();
    private static ConcurrentHashMap<String, StoreEntry> ttlEntries;

    public TTLManager(ConcurrentHashMap<String, StoreEntry> ttlentries,CountDownLatch shutdownLatch) {
        this.shutdownLatch = shutdownLatch;
        ttlEntries = ttlentries;
    }

    public void stopIt() {
        running = false;
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }
        shutdownLatch.countDown();
    }

    @Override
    public void run() {
        System.out.println("---- Starting TTL lazy eviction :P ----");
        scheduler.scheduleAtFixedRate(() -> {
            if (!running) return;

            int expiredCount = 0;
            int sampleSize = Math.min(20, ttlEntries.size());
            if (sampleSize == 0) return;

            // sample random entries
            List<StoreEntry> values = ttlEntries.values().stream().toList();
            for (int i = 0; i < sampleSize; i++) {
                StoreEntry se = values.get(random.nextInt(values.size()));
                if (se.TTL && System.currentTimeMillis() > se.expiryTime)
                    expiredCount++;
            }

            // if enough expired, trigger full scan
            if (expiredCount >= Math.max(5, sampleSize / 4)) {
                System.out.println("----- Found expired TTL entries. Running full scan -----");
                ttlChecker();
            }

        }, 0, 10, TimeUnit.SECONDS);
    }

    // full scan removes expired entries from the actual ttlEntries map
    private void ttlChecker() {
        Iterator<StoreEntry> it = ttlEntries.values().iterator();
        long now = System.currentTimeMillis();
        int removed = 0;

        while (it.hasNext()) {
            StoreEntry se = it.next();
            if (se.TTL && now >= se.expiryTime) {
                it.remove();
                removed++;
            }
        }
        if (removed > 0) 
            System.out.println("TTLManager removed " + removed + " expired keys.");
    }
}
