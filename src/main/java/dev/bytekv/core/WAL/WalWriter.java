package dev.bytekv.core.WAL;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public final class WalWriter implements Runnable {

    private final WalController wal;
    private final BlockingQueue<WalEntry> queue;
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final AtomicBoolean hasFailed = new AtomicBoolean(false);

    public WalWriter(WalController wal) {
        this.wal = wal;
        this.queue = new LinkedBlockingQueue<>(5000); 
    }

    public void append(WalEntry entry) {
        if (hasFailed.get()) {
            throw new RuntimeException("WAL Writer is down, refusing writes");
        }
        queue.add(entry);
    }

    public void shutdown() {
        running.set(false);
    }

    @Override
    public void run() {
        try {
            while (running.get() || !queue.isEmpty()) {
                WalEntry entry = queue.poll(100, TimeUnit.MILLISECONDS);
                
                if (entry != null) {
                    wal.writeToFile(entry);
                }
            }
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        catch (IOException e) {
            hasFailed.set(true);
        } 

        finally {
            try{
                wal.close();
            }
            catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}