package dev.bytekv.core.storage;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import java.util.concurrent.atomic.AtomicInteger;

import java.io.BufferedReader;
import java.nio.file.Path;


import java.util.Collections;

public class SSTManager {

    private static AtomicInteger sstCounter = new AtomicInteger(0);

    private final List<SSTable> sstableList = Collections.synchronizedList(new ArrayList<>());

    public AtomicInteger getSSTCount(){
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

        sstableList.add(0, sst);

        System.out.println("Flushed memtable to SSTable #" + id);
    }

    public List<SSTable> getSSTables() {
        return Collections.unmodifiableList(sstableList);

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

    public void mergeSST() throws IOException {
    if (sstableList.size() < 2) return;

    SSTable oldSST = sstableList.get(sstableList.size() - 1);
    SSTable newSST = sstableList.get(sstableList.size() - 2);

    Path oldPath = Paths.get("SST", oldSST.getSSTName(), oldSST.getSSTName() + ".data");
    Path newPath = Paths.get("SST", newSST.getSSTName(), newSST.getSSTName() + ".data");

    SSTable mergedSST = new SSTable(sstCounter.incrementAndGet());

    try (
        BufferedReader reader1 = Files.newBufferedReader(oldPath);
        BufferedReader reader2 = Files.newBufferedReader(newPath);
    ) {
        String line1 = reader1.readLine();
        String line2 = reader2.readLine();

        while (line1 != null && line2 != null) {
            String[] entry1 = line1.split(",", 2);
            String[] entry2 = line2.split(",", 2);

            String key1 = entry1[0];
            String key2 = entry2[0];

            if (key1.equals(key2)) {
                mergedSST.put(key2, entry2[1]);
                line1 = reader1.readLine();
                line2 = reader2.readLine();
            } else if (key1.compareTo(key2) < 0) {
                mergedSST.put(key1, entry1[1]);
                line1 = reader1.readLine();
            } else {
                mergedSST.put(key2, entry2[1]);
                line2 = reader2.readLine();
            }
        }

        while (line1 != null) {
            String[] entry = line1.split(",", 2);
            mergedSST.put(entry[0], entry[1]);
            line1 = reader1.readLine();
        }

        while (line2 != null) {
            String[] entry = line2.split(",", 2);
            mergedSST.put(entry[0], entry[1]);
            line2 = reader2.readLine();
        }
    }

    System.out.println("Merged and deleted: " + oldSST.getSSTName() + " and " + newSST.getSSTName());

    deleteFolder(Paths.get("SST", oldSST.getSSTName()).toFile());
    deleteFolder(Paths.get("SST", newSST.getSSTName()).toFile());

    sstableList.remove(oldSST);
    sstableList.remove(newSST);
}


}
