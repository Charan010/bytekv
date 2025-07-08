package dev.bytekv.core;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class SSTManager {

    private static int sstCounter = 0;
    private final List<SSTable> sstableList = new ArrayList<>();

    public void addSSTable(SSTable newTable) {
        String newName = newTable.getSSTName();
        int i = 0;
        while (i < sstableList.size() &&
           sstableList.get(i).getSSTName().compareTo(newName) > 0) {
            i++;
        }
        sstableList.add(i, newTable);
    }

    public static int getSSTCount(){
        return sstCounter;
    }

    public void flushToSSTable(TreeMap<String, String> memtable) throws IOException {
        sstCounter++;

        SSTable sst = new SSTable(sstCounter);
        for (Map.Entry<String, String> entry : memtable.entrySet()) {
            sst.put(entry.getKey(), entry.getValue());
        }

        sst.flush();
        sst.loadFromIndexFile();

        addSSTable(sst);

        System.out.println("Flushed memtable to SSTable #" + sstCounter);
    }

    public List<SSTable> getSSTables() {
        return sstableList;
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
        
        if(sstableList.size() < 2)
            return ;

        SSTable oldSST = sstableList.get(sstableList.size()-1);
        SSTable newSST = sstableList.get(sstableList.size()-2);

        List<String> oldLines = Files.readAllLines(Paths.get("SST/" + oldSST.getSSTName() + "/" + oldSST.getSSTName() + ".data"));
        List<String> newLines = Files.readAllLines(Paths.get("SST/" + newSST.getSSTName() + "/" + newSST.getSSTName() + ".data"));

        SSTable mergedSST = new SSTable(++sstCounter);

        int i = 0, j = 0;

        while (i < oldLines.size() && j < newLines.size()) {
            String[] oldEntry = oldLines.get(i).split(",", 2);
            String[] newEntry = newLines.get(j).split(",", 2);

            String oldKey = oldEntry[0];
            String newKey = newEntry[0];

            if (oldKey.equals(newKey)) {
                mergedSST.put(newKey, newEntry[1]);
                i++;
                j++;
            } else if (oldKey.compareTo(newKey) < 0) {
                mergedSST.put(oldKey, oldEntry[1]);
                i++;
            } else {
                mergedSST.put(newKey, newEntry[1]);
                j++;
            }
        }

        while (i < oldLines.size()) {
            String[] oldEntry = oldLines.get(i++).split(",", 2);
            mergedSST.put(oldEntry[0], oldEntry[1]);
        }

        while (j < newLines.size()) {
            String[] newEntry = newLines.get(j++).split(",", 2);
            mergedSST.put(newEntry[0], newEntry[1]);
        }

        System.out.println("Merged and deleted: " + oldSST.getSSTName() + " and " + newSST.getSSTName());

        deleteFolder(Paths.get("SST", oldSST.getSSTName()).toFile());
        deleteFolder(Paths.get("SST", newSST.getSSTName()).toFile());

        sstableList.remove(oldSST);
        sstableList.remove(newSST);
    }

}
