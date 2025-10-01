package dev.bytekv.core.storage;

import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import dev.bytekv.proto.SSTWriteOuterClass;


public class SSTManager {

    private static AtomicInteger sstCounter = new AtomicInteger(0);

    private final Map<Integer, List<SSTable>> levels = new TreeMap<>();
    private final int MAX_SST_PER_LEVEL = 4;

    public AtomicInteger getSSTCount() {
        return sstCounter;
    }

    public SSTManager() throws IOException{   
        repopulateIndexes();
    }

    public void repopulateIndexes() throws IOException {
        File dir = new File("SST");
        File[] folders = dir.listFiles(File::isDirectory);

        if (folders == null || folders.length == 0) {
            System.out.println("--- No SSTables found ---");
            return;
        }

        sstCounter.set(0);
        levels.clear();

        Arrays.sort(folders, Comparator.comparingInt(f -> {
            String name = f.getName();
            String[] vals = name.split("-");
            return Integer.parseInt(vals[1]);
        }));

        for (File folder : folders) {
            String oldName = folder.getName();
            String[] vals = oldName.split("-");
            if (vals.length < 2) continue;

            int newId = sstCounter.incrementAndGet();
            String newPadded = String.format("%03d", newId);
            String newName = "sstable-" + newPadded;

            File newFolder = new File(dir, newName);

            if (!oldName.equals(newName)) {
                if (!folder.renameTo(newFolder)) {
                    throw new IOException("Failed to rename folder " + oldName + " -> " + newName);
                }
            } else {
                newFolder = folder; 
            }

            File oldData = new File(newFolder, oldName + ".data");
            File oldIndex = new File(newFolder, oldName + ".index");
            File newData = new File(newFolder, newName + ".data");
            File newIndex = new File(newFolder, newName + ".index");

            if (oldData.exists() && !oldData.renameTo(newData)) {
                throw new IOException("Failed to rename data file: " + oldData.getName());
            }
            if (oldIndex.exists() && !oldIndex.renameTo(newIndex)) {
                throw new IOException("Failed to rename index file: " + oldIndex.getName());
            }

            SSTable sst = new SSTable(newFolder);
            levels.computeIfAbsent(0, k -> new ArrayList<>()).add(sst);
        }

        System.out.println("Repopulated SSTables, total: " + sstCounter.get());
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

    private synchronized void tieredCompaction() throws IOException {
        for (int level : new ArrayList<>(levels.keySet())) {
            List<SSTable> ssts = levels.get(level);

            while (ssts.size() >= MAX_SST_PER_LEVEL) {
                List<SSTable> toMerge = new ArrayList<>(ssts.subList(0, MAX_SST_PER_LEVEL));
                SSTable merged = mergeSSTables(toMerge);

                for (SSTable sst : toMerge) {
                    deleteFolder(Paths.get("SST", sst.getSSTName()).toFile());
                }

                ssts.subList(0, MAX_SST_PER_LEVEL).clear();

                levels.computeIfAbsent(level + 1, k -> new ArrayList<>()).add(merged);

                System.out.println("Promoted merged SSTable to level " + (level + 1));
            }
        }
    }

    private SSTable mergeSSTables(List<SSTable> sstables) throws IOException {
        SSTable merged = new SSTable(sstCounter.incrementAndGet());
        List<DataInputStream> streams = new ArrayList<>();
        PriorityQueue<Entry> pq = new PriorityQueue<>(Comparator.comparing(e -> e.key));

        try {
            for (SSTable sst : sstables) {
                Path path = Paths.get("SST", sst.getSSTName(), sst.getSSTName() + ".data");
                DataInputStream dis = new DataInputStream(new FileInputStream(path.toFile()));
                streams.add(dis);

                if (dis.available() > 0) {
                    int len = dis.readInt();
                    byte[] buf = new byte[len];
                    dis.readFully(buf);
                    SSTWriteOuterClass.SSTWrite msg = SSTWriteOuterClass.SSTWrite.parseFrom(buf);

                    pq.add(new Entry(msg.getKey(), msg.getValue(), dis));
                }
            }

            while (!pq.isEmpty()) {
                Entry smallest = pq.poll();
                merged.put(smallest.key, smallest.value);

                if (smallest.stream.available() > 0) {
                    int len = smallest.stream.readInt();
                    byte[] buf = new byte[len];
                    smallest.stream.readFully(buf);
                    SSTWriteOuterClass.SSTWrite msg = SSTWriteOuterClass.SSTWrite.parseFrom(buf);

                    pq.add(new Entry(msg.getKey(), msg.getValue(), smallest.stream));
                }
            }
        } finally {
            for (DataInputStream dis : streams) dis.close();
        }

        merged.flush();
        merged.loadFromIndexFile();
        return merged;
    }

    private static class Entry {
        String key, value;
        DataInputStream stream;

        Entry(String key, String value, DataInputStream stream) {
            this.key = key;
            this.value = value;
            this.stream = stream;
        }
    }
}
