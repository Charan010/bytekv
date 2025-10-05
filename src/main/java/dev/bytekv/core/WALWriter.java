package dev.bytekv.core;

import dev.bytekv.proto.LogEntryOuterClass;

import java.io.File;
import java.io.FileOutputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.CompletableFuture;


public class WALWriter {
    private final FileOutputStream fos;
    private final BufferedOutputStream bos;  

    private final LinkedBlockingQueue<LogEntryOuterClass.LogEntry> queue;
    private final Thread writerThread;
    private final Thread flushThread;
    private volatile boolean running = true;

    private final int MAX_BATCH_SIZE = 200;  
    private final int FLUSH_INTERVAL_MS = 200;   
    private final int QUEUE_CAPACITY = 10_000;   
    private final int SYNC_INTERVAL_MS = 500;
    private final int BUFFER_SIZE = 128 * 1024;


    /*
        * MAX_BATCH_SIZE - amount of entries to be drained from queue to batch write to WAL file.
        * FLUSH_INTERVAL_MS - periodic time to flush which forces to JVM cache to be flushed to OS.
     
       * SYNC_INTERVAL_MS - This is a blocking task which forces data to be persisted to harddisk and wait for acknowledgement.
         Tradeoffs:
        - If the SNYC_INTERVAL_MS is too less, then it would lead to lot of work and if it's more then there is chance of data loss
        when database crashes.
                
       * BUFFER_SIZE - maximum amount of data BufferedOutputStream can take to reduce number of system calls.
    */

    public WALWriter(String logFilePath) throws IOException {
        File f = new File(logFilePath);
        f.getParentFile().mkdirs();

        this.fos = new FileOutputStream(f, true);
        this.queue = new LinkedBlockingQueue<>(QUEUE_CAPACITY);
        this.bos = new BufferedOutputStream(fos, BUFFER_SIZE);  

        writerThread = new Thread(this::processLoop, "WAL-Writer");
        writerThread.start();

        flushThread = new Thread(this::periodicFlushLoop, "WAL-Periodic-Flush");
        flushThread.start();
    }

    public void writeToLog(LogEntryOuterClass.LogEntry entry) throws IOException, InterruptedException {
            if (!queue.offer(entry, 30, TimeUnit.MILLISECONDS))
                throw new IOException("WAL queue overflow");
    }

    private void processLoop() {
        List<LogEntryOuterClass.LogEntry> batch = new ArrayList<>(MAX_BATCH_SIZE);
        try {
            while (running || !queue.isEmpty()) {
                batch.clear();

            /*
              * Once, queue has enough entries, drain all entries upto MAX_BATCH_SIZE to be written in log file batch wise. 
            */
                queue.drainTo(batch, MAX_BATCH_SIZE);

                if (batch.isEmpty()) {
                    Thread.sleep(5); 
                    continue;
                }
                for (LogEntryOuterClass.LogEntry entry : batch) {
                    entry.writeDelimitedTo(bos);
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
                synchronized (bos) {
                bos.flush();
                }

                long now = System.currentTimeMillis();
                if (now - lastSyncTime >= SYNC_INTERVAL_MS) {

    /*
        * fos.getFD().sync() is a blocking thread which force flushes os page cache as well to make sure the data is actually persisted
         in the disk. This is time consuming because it waits until OS sends some kind of acknowlegement that data is succesfully stored in
         the disk. So, I'm asynchronously force flushing.
    */
                    CompletableFuture.runAsync(this::sync); 
                    lastSyncTime = now;
                }
            }
        }   catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void sync() {
        try {
            synchronized (bos) {
                bos.flush();
                fos.getFD().sync();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    
    public void shutDown() throws IOException, InterruptedException {
        running = false;
        writerThread.join();
        flushThread.join();

        synchronized (bos) {
            bos.flush();
            fos.getFD().sync();
            bos.close();
        }
    }
}
