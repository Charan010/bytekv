package dev.bytekv;

import dev.bytekv.core.KVStore;
import org.HdrHistogram.Histogram;

import java.util.Random;
import java.util.concurrent.*;

public class BenchMark {

    private static final int TOTAL_OPS = 200_000; // realistic volume
    private static final int THREADS = 8;
    private static final double READ_RATIO = 0.7; // 70% reads, 30% writes
    private static final int KEY_SPACE = 50_000; // smaller than total ops -> reuse = cache hits
    private static final int TTL_PERCENT = 20;   // 20% of writes have TTL
    private static final int TTL_MAX_MS = 10_000;

    public static void main(String[] args) throws Exception {
        KVStore kv = KVStore.createDB(30, 150, "logs", 3500);
        ExecutorService exec = Executors.newFixedThreadPool(THREADS);
        Random rand = new Random();

        System.out.println("Warming up cache and log layers...");
        for (int i = 0; i < KEY_SPACE; i++) {
            kv.put("warmup" + i, "value" + i).get();
        }

        System.out.println("Starting mixed workload benchmark...");
        benchmarkMixed(kv, exec, rand);

        exec.shutdown();
        kv.shutDown();
    }

    private static void benchmarkMixed(KVStore kv, ExecutorService exec, Random rand) throws InterruptedException {
        Histogram hist = new Histogram(3600000000000L, 3);
        CountDownLatch latch = new CountDownLatch(TOTAL_OPS);
        long start = System.nanoTime();

        for (int i = 0; i < TOTAL_OPS; i++) {
            exec.submit(() -> {
                try {
                    long t0 = System.nanoTime();
                    if (rand.nextDouble() < READ_RATIO) {
                        // perform GET
                        int keyId = rand.nextInt(KEY_SPACE);
                        kv.getTTL("key" + keyId).get();
                    } else {
                        // perform PUT (some with TTL)
                        int keyId = rand.nextInt(KEY_SPACE);
                        String val = "val" + rand.nextInt(1_000_000);
                        if (rand.nextInt(100) < TTL_PERCENT) {
                            int ttl = rand.nextInt(TTL_MAX_MS) + 1000;
                            kv.put("key" + keyId, val, ttl).get();
                        } else {
                            kv.put("key" + keyId, val).get();
                        }
                    }
                    hist.recordValue(System.nanoTime() - t0);
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    latch.countDown();
                }
            });
        }

        latch.await();
        long end = System.nanoTime();

        double durationSec = (end - start) / 1e9;
        double throughput = TOTAL_OPS / durationSec;

        System.out.printf("\n📊 Mixed workload results (%.0f%% read / %.0f%% write)\n", 
            READ_RATIO * 100, (1 - READ_RATIO) * 100);
        System.out.printf("Total ops: %d\n", TOTAL_OPS);
        System.out.printf("Duration: %.2f s\n", durationSec);
        System.out.printf("Throughput: %.2f ops/sec\n", throughput);

        printHistogram(hist);
    }

    private static void printHistogram(Histogram hist) {
        System.out.printf(
            "Avg latency: %.2f µs | Median: %.2f µs | 95th: %.2f µs | 99th: %.2f µs | Max: %.2f ms\n",
            hist.getMean() / 1e3,
            hist.getValueAtPercentile(50) / 1e3,
            hist.getValueAtPercentile(95) / 1e3,
            hist.getValueAtPercentile(99) / 1e3,
            hist.getMaxValue() / 1e6
        );
    }
}
