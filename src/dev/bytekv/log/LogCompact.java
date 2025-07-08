package dev.bytekv.log;

/*
    Logging of every key&value is fine. but it would be clunky and lot of logs when we are dealing with lot of values
    so. compaction would be triggered when log reaches 1000 records and removes duplicates .

    this "1000" can be adjusted in MAX_ENTRIES

 */


import dev.bytekv.proto.LogEntryOuterClass;

import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class LogCompact implements Runnable {

    private final String logFilePath;
    private final String logDir;
    private static final long MAX_ENTRIES = 1000;

    public LogCompact(String logFilePath, String logDir) {
        this.logFilePath = logFilePath;
        this.logDir = logDir;
    }

    public void run() {
        System.out.println("🧹 [LogCompactor] Starting background log compaction scheduler...");

        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

        scheduler.scheduleAtFixedRate(() -> {
            try {
                long currentEntries = LogEntry.returnSerialNumber();
                if (currentEntries > MAX_ENTRIES) {
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
                String op = entry.getOperation();

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
    }
}