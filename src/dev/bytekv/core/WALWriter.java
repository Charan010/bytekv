package dev.bytekv.core;

import dev.bytekv.proto.LogEntryOuterClass;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class WALWriter {
    private final FileOutputStream fos;
    private final LinkedBlockingQueue<LogEntryOuterClass.LogEntry> queue;
    private final Thread writerThread;
    private final Thread flushThread;
    private volatile boolean running = true;
    private final boolean backPressureOn;

    private final int MAX_BATCH_SIZE = 200;  
    private final int FLUSH_INTERVAL_MS = 100;   
    private final int QUEUE_CAPACITY = 20_000;   
    private final int SYNC_INTERVAL_MS = 1000;

    public WALWriter(String logFilePath, boolean backPressureOn) throws IOException {
        File f = new File(logFilePath);
        f.getParentFile().mkdirs();

        this.fos = new FileOutputStream(f, true);
        this.queue = new LinkedBlockingQueue<>(QUEUE_CAPACITY);
        this.backPressureOn = backPressureOn;

        writerThread = new Thread(this::processLoop, "WAL-Writer");
        writerThread.start();

        flushThread = new Thread(this::periodicFlushLoop, "WAL-Periodic-Flush");
        flushThread.start();
    }

    public void writeToLog(LogEntryOuterClass.LogEntry entry) throws IOException, InterruptedException {
        if (backPressureOn) {
            if (!queue.offer(entry, 30, TimeUnit.MILLISECONDS))
                throw new IOException("WAL queue overflow (backpressure applied)");
            
        } else {
            if (!queue.offer(entry)) 
                throw new IOException("WAL queue overflow");
        }
    }

    private void processLoop() {
        List<LogEntryOuterClass.LogEntry> batch = new ArrayList<>(MAX_BATCH_SIZE);
        try {
            while (running || !queue.isEmpty()) {
                batch.clear();
                queue.drainTo(batch, MAX_BATCH_SIZE);

                if (batch.isEmpty()) {
                    Thread.sleep(5); 
                    continue;
                }

                synchronized (fos) {
                    for (LogEntryOuterClass.LogEntry entry : batch) {
                        entry.writeDelimitedTo(fos);
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void periodicFlushLoop() {
        long lastSyncTime = System.currentTimeMillis();

        try {
            while (running) {
                Thread.sleep(FLUSH_INTERVAL_MS);
                synchronized (fos) {
                    fos.flush();

                long now = System.currentTimeMillis();
                if (now - lastSyncTime >= SYNC_INTERVAL_MS) {
                        fos.getFD().sync();
                        lastSyncTime = now;
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void shutDown() throws IOException, InterruptedException {
        running = false;
        writerThread.join();
        flushThread.join();

        synchronized (fos) {
            fos.flush();
            fos.getFD().sync();
            fos.close();
        }
    }
}
