package dev.bytekv;

import dev.bytekv.core.KVStore;
import java.util.Random;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

public class BenchMark {

    public static void main(String[] args) throws Exception {
        KVStore kv = KVStore.createDB(30, 100, "logs", 3500);
        //kv.replayLogs();
        kv.logging(true);

        ExecutorService exec = Executors.newFixedThreadPool(8);
        Random rand = new Random();

        int puts = 100_000;
        int gets = 50_000;

        AtomicLong putSumNano = new AtomicLong(0);
        AtomicLong putMaxNano = new AtomicLong(0);
        AtomicLong getSumNano = new AtomicLong(0);
        AtomicLong getMaxNano = new AtomicLong(0);
        AtomicLong ttlExpiredCount = new AtomicLong(0);

        CountDownLatch latch = new CountDownLatch(puts);
        long start = System.nanoTime();

        for (int i = 0; i < puts; i++) {
            int id = i;
            exec.submit(() -> {
                try {
                    long putStart = System.nanoTime();
                    if (id % 2 == 0) kv
                        .put("key" + id, "value" + id, 5000)
                        .get();
                    else kv.put("key" + id, "value" + id).get();
                    long putEnd = System.nanoTime();
                    long duration = putEnd - putStart;

                    putSumNano.addAndGet(duration);
                    putMaxNano.updateAndGet(prev -> Math.max(prev, duration));
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    latch.countDown();
                }
            });
        }

        latch.await();
        long end = System.nanoTime();
        double putThroughput = puts / ((end - start) / 1e9);

        System.out.println("Waiting 6s for TTL expirations...");
        Thread.sleep(6000);

        for (int i = 0; i < gets; i++) {
            int id = rand.nextInt(puts);
            long getStart = System.nanoTime();
            try {
                String val = kv.getTTL("key" + id).get();
                long getEnd = System.nanoTime();
                long duration = getEnd - getStart;

                getSumNano.addAndGet(duration);
                getMaxNano.updateAndGet(prev -> Math.max(prev, duration));

                if (val == null) ttlExpiredCount.incrementAndGet();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        exec.shutdown();

        System.out.println("\n=== ByteKV Benchmark Results ===");
        System.out.printf(
            "PUTs: %d ops, avg latency %.2f µs, max latency %.2f ms, throughput %.2f ops/sec%n",
            puts,
            putSumNano.get() / 1e3 / puts,
            putMaxNano.get() / 1e6,
            putThroughput
        );
        System.out.printf(
            "GETs: %d ops, avg latency %.2f µs, max latency %.2f ms%n",
            gets,
            getSumNano.get() / 1e3 / gets,
            getMaxNano.get() / 1e6
        );
        System.out.println("TTL expired keys seen: " + ttlExpiredCount.get());

        kv.shutDown();

    }
}
