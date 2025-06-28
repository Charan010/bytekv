package src.dev.meshkv;


import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

class LogEntry {
    public enum OpType { PUT, DELETE }

    public OpType type;
    public String key;
    public String value;

    public LogEntry(OpType type, String key, String value) {
        this.type = type;
        this.key = key;
        this.value = value;
    }

    public static LogEntry parse(String line) {
        String[] parts = line.split(" ", 3);
        if (parts[0].equals("PUT")) {
            return new LogEntry(OpType.PUT, parts[1], parts[2]);
        } else if (parts[0].equals("DELETE")) {
            return new LogEntry(OpType.DELETE, parts[1], null);
        } else {
            throw new IllegalArgumentException("Invalid log entry: " + line);
        }
    }

    @Override
    public String toString() {
        return (type == OpType.PUT) ? "PUT " + key + " " + value : "DELETE " + key;
    }
}

/* A daemon thread which would trigger log compaction to reduce size of the 
log file when log file gets large */


public class LogJanitor implements Runnable {
    private final long maxLogSizeBytes = 50 * 1024 * 1024; // 50 MB
    private final int compactionDeltaThreshold = 1000; // not used yet
    private String lastCompactedAt = null;
    private final String logPath;

    public LogJanitor(String logPath) {
        this.logPath = logPath;
    }

    @Override
    public void run() {
        File file = new File(this.logPath);
        while (true) {
            if (file.length() > maxLogSizeBytes) {
                lastCompactedAt = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd_HH-mm-ss"));
                compactLog(this.logPath);
            }

            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                System.err.println("[WARN] LogJanitor interrupted: " + e.getMessage());
                Thread.currentThread().interrupt();
            }
        }
    }

    private static void compactLog(String logPath) {
        System.out.println("[INFO] Triggering log compaction...");

        Path originalPath = Paths.get(logPath);
        Path tempPath = Paths.get(logPath + ".tmp");

        List<String> allLines;
        try {
            allLines = Files.readAllLines(originalPath);
        } catch (IOException e) {
            System.err.println("[ERROR] Failed to read log: " + e.getMessage());
            return;
        }

        Map<String, LogEntry> Betterop = new LinkedHashMap<>();
        for (String line : allLines) {
            LogEntry entry = LogEntry.parse(line);
            Betterop.put(entry.key, entry);
        }

        List<String> compactedLines = Betterop.values().stream()
                .filter(e -> e.type != LogEntry.OpType.DELETE)
                .map(LogEntry::toString)
                .collect(Collectors.toList());

        try {
            Files.write(tempPath, compactedLines);
            Files.move(tempPath, originalPath, StandardCopyOption.REPLACE_EXISTING);
            System.out.println("[INFO] Log compaction successful!");
        } catch (IOException e) {
            System.err.println("[ERROR] Failed to write or move compacted log: " + e.getMessage());
        }
    }
}




