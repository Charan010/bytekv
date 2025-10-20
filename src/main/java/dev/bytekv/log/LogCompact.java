package dev.bytekv.log;

/*
    * To decrease WAL file size, log compaction could be triggered when number of entries exceed x threshold where stale data is overwritten
    and log file is updated.

    * Tradeoffs:
    - LogCompaction is implemented under assumption that data gets stale or overwritten a lot so older data could be overwritten to reduce log size.
    - Snapshotting is much better choice if we are dealing with normal data.

 */

import dev.bytekv.proto.LogEntryOuterClass;

/* 
import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.CountDownLatch;

public class LogCompact implements Runnable {

    private final String logFilePath;
    private final String logDir = "";
    private static final long MAX_ENTRIES = 1000;
    private volatile boolean running = true;
    private CountDownLatch shutdownLatch;

    public LogCompact(String logFilePath) {
        this.logFilePath = logFilePath;
    }

    public void stopIt(){
        running = false;
        shutdownLatch.countDown();
    }   

    public void run() {
        System.out.println("[LogCompactor] Starting background log compaction scheduler...");

        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

        scheduler.scheduleAtFixedRate(() -> {
            if(!running)
                return;
                
            try {
                long currentEntries = LogEntry.returnSerialNumber();
                if (currentEntries > MAX_ENTRIES) {
                    //cOn = true;
                    compactLog();
                }
            } catch (Exception e) {
                System.out.println("[LogCompactor] Error during scheduled compaction: " + e.getMessage());
            }
        }, 0, 5, TimeUnit.MINUTES);
    }

    public void compactLog() {

        Map<String, LogEntryOuterClass.LogEntry> latestOps = new HashMap<>();

        try (FileInputStream fis = new FileInputStream(logFilePath)) {
            while (true) {
                LogEntryOuterClass.LogEntry entry = LogEntryOuterClass.LogEntry.parseDelimitedFrom(fis);
                if (entry == null) break;

                String key = entry.getKey();
                LogEntryOuterClass.LogEntry.Operation protoOp = entry.getOp();

                String op = "";
                switch (protoOp) {
                    case PUT:
                        op = "PUT";
                        break;
                    case DELETE:
                        op = "DELETE";
                        break;
                }

                if (op.equals("DELETE")) {
                    latestOps.remove(key); 
                } else {
                    latestOps.put(key, entry);
                }
            }
        } catch (IOException e) {
            System.out.println("[LogCompactor] Failed to read log: " + e.getMessage());
            return;
        }

        Path compactedPath = Paths.get(logDir, "compacted.log");
        try (FileOutputStream fos = new FileOutputStream(compactedPath.toFile())) {
            for (LogEntryOuterClass.LogEntry entry : latestOps.values()) {
                entry.writeDelimitedTo(fos);
            }
        } catch (IOException e) {
            System.out.println("[LogCompactor] Failed to write compacted log: " + e.getMessage());
            return;
        }

        // Replace original log with compacted one
        try {
            Files.move(compactedPath, Paths.get(logFilePath), StandardCopyOption.REPLACE_EXISTING);
            System.out.println("[LogCompactor] Log compaction successful.");
        } catch (IOException e) {
            System.out.println("[LogCompactor] Failed to replace original log: " + e.getMessage());
        }
        
        finally{
         System.out.println("do nothing");   
        }
    }
}

*/