package dev.bytekv.core;

import dev.bytekv.proto.LogEntryOuterClass;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

public class WALWriter {
    private  FileOutputStream fos;
    private  BufferedOutputStream bos;
    private LinkedBlockingQueue<LogEntryOuterClass.LogEntry> queue;

    private final ExecutorService writerExecutor;
    private final ScheduledExecutorService flushExecutor;

    private volatile boolean failed = false;
    private volatile boolean running = true;

    String logFilePath;
    private File f; 

    private final int MAX_BATCH_SIZE = 200;
    private final int QUEUE_CAPACITY = 10_000;
    private final int FLUSH_INTERVAL_MS = 200;
    private final int SYNC_INTERVAL_MS = 500;
    private final int BUFFER_SIZE = 128 * 1024;
    private CountDownLatch shutCountDownLatch;
    
    public WALWriter(String logFilePath, CountDownLatch countDownLatch) throws IOException {
        f = new File(logFilePath);
        f.getParentFile().mkdirs();

        shutCountDownLatch = countDownLatch;

        this.logFilePath = logFilePath;
        fos = new FileOutputStream(f, true);
        bos = new BufferedOutputStream(fos, BUFFER_SIZE);
        queue = new LinkedBlockingQueue<>(QUEUE_CAPACITY);

        writerExecutor = Executors.newSingleThreadExecutor(r -> new Thread(r, "WAL-Writer"));
        flushExecutor = Executors.newSingleThreadScheduledExecutor(r -> new Thread(r, "WAL-FlushSync"));

        writerExecutor.submit(this::processLoop);
        flushExecutor.scheduleAtFixedRate(this::flushAndSync, FLUSH_INTERVAL_MS, FLUSH_INTERVAL_MS, TimeUnit.MILLISECONDS);
    }

    public void stopIt(){
        running = false;
    }




    public void writeToLog(LogEntryOuterClass.LogEntry entry) throws IOException {
        if (failed) throw new IOException("WAL in failed state!");
        try {
            if (!queue.offer(entry, 30, TimeUnit.MILLISECONDS)) {
                throw new IOException("WAL queue overflow");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while enqueuing WAL entry", e);
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

                synchronized (bos) {
                    for (LogEntryOuterClass.LogEntry entry : batch) {
                        entry.writeDelimitedTo(bos);
                    }
                }
            }
        } catch (Exception e) {
            failed = true;
            e.printStackTrace();

        }finally{
            shutCountDownLatch.countDown();
        }
    }

    
    private void flushAndSync() {
        try {
            synchronized (bos) {
                bos.flush();
            }

            long now = System.currentTimeMillis();
            if (now % SYNC_INTERVAL_MS < FLUSH_INTERVAL_MS) {
                synchronized (bos) {

        /*
            * fos.getFD().sync() is a blocking thread which force flushes os page cache as well to make sure the data is actually persisted
            in the disk. This is time consuming because it waits until OS sends some kind of acknowlegement that data is succesfully stored in
            the disk. So, I'm asynchronously force flushing.
        */
                    fos.getFD().sync();
                }
            }
        } catch (IOException e) {
            failed = true;
            e.printStackTrace();
        }
    }

    public void truncateWALFile() throws IOException {
        System.out.println("Truncating WAL file :///");
        synchronized (bos) {
            bos.flush();
            bos.close();
            fos.close();
        }

        new FileOutputStream(logFilePath, false).close();
        this.fos = new FileOutputStream(f, true);
        this.bos = new BufferedOutputStream(fos, BUFFER_SIZE);
    }


    public void shutDown() throws IOException {
        running = false;

        writerExecutor.shutdown();
        flushExecutor.shutdown();

        try {
            writerExecutor.awaitTermination(5, TimeUnit.SECONDS);
            flushExecutor.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        synchronized (bos) {
            bos.flush();
            fos.getFD().sync();
            bos.close();
        }
    }

    public boolean isFailed() {
        return failed;
    }
}
