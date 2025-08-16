package dev.bytekv.core.storage;

import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class SSTManager {

    private static AtomicInteger sstCounter = new AtomicInteger(0);

    private final Map<Integer, List<SSTable>> levels = new TreeMap<>();
    private final int MAX_SST_PER_LEVEL = 4;

    public AtomicInteger getSSTCount() {
        return sstCounter;
        
    }

    public void flushToSSTable(TreeMap<String, String> memtable) throws IOException {
        int id = sstCounter.incrementAndGet();
        SSTable sst = new SSTable(id);

        for (Map.Entry<String, String> entry : memtable.entrySet()) {
            sst.put(entry.getKey(), entry.getValue());
        }

        sst.flush();
        sst.loadFromIndexFile();

        levels.computeIfAbsent(0, k -> new ArrayList<>()).add(sst);

        System.out.println("Flushed memtable to SSTable #" + id);

        tieredCompaction();
    }

    public List<SSTable> getAllSSTables() {
        List<SSTable> all = new ArrayList<>();
        levels.values().forEach(all::addAll);
        return Collections.unmodifiableList(all);
    }

    public static void deleteFolder(File folder) {
        File[] files = folder.listFiles();
        if (files != null) {
            for (File file : files) {
                if (file.isDirectory())
                    deleteFolder(file);
                else
                    file.delete();
            }
        }
        folder.delete();
    }

    private void tieredCompaction() throws IOException {
        for (int level : new ArrayList<>(levels.keySet())) {
            List<SSTable> ssts = levels.get(level);

            while (ssts.size() >= MAX_SST_PER_LEVEL) {

                List<SSTable> toMerge = new ArrayList<>(ssts.subList(0, MAX_SST_PER_LEVEL));
                SSTable merged = mergeSSTables(toMerge);

                for (SSTable sst : toMerge) {
                    deleteFolder(Paths.get("SST", sst.getSSTName()).toFile());
                }

                ssts.subList(0, MAX_SST_PER_LEVEL).clear();

                // Promote merged SSTable to next level
                levels.computeIfAbsent(level + 1, k -> new ArrayList<>()).add(merged);

                System.out.println("Promoted merged SSTable to level " + (level + 1));
            }
        }
    }

    private SSTable mergeSSTables(List<SSTable> sstables) throws IOException {
        SSTable merged = new SSTable(sstCounter.incrementAndGet());
        List<BufferedReader> readers = new ArrayList<>();
        PriorityQueue<Entry> pq = new PriorityQueue<>(Comparator.comparing(e -> e.key));

        try {
            for (SSTable sst : sstables) {
                Path path = Paths.get("SST", sst.getSSTName(), sst.getSSTName() + ".data");
                BufferedReader reader = Files.newBufferedReader(path);
                readers.add(reader);

                String line = reader.readLine();
                if (line != null) {
                    String[] parts = line.split(",", 2);
                    pq.add(new Entry(parts[0], parts[1], reader));
                }
            }

            while (!pq.isEmpty()) {
                Entry smallest = pq.poll();
                merged.put(smallest.key, smallest.value);

                String nextLine = smallest.reader.readLine();
                if (nextLine != null) {
                    String[] parts = nextLine.split(",", 2);
                    pq.add(new Entry(parts[0], parts[1], smallest.reader));
                }
            }
        } finally {
            for (BufferedReader reader : readers) {
                reader.close();
            }
        }

        merged.flush();
        merged.loadFromIndexFile();
        return merged;
    }

    private static class Entry {
        String key, value;
        BufferedReader reader;

        Entry(String key, String value, BufferedReader reader) {
            this.key = key;
            this.value = value;
            this.reader = reader;
        }
    }
}
