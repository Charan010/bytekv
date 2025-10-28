package dev.bytekv.core.storage;

import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import dev.bytekv.proto.SSTWriteOuterClass;

/*
    * Time complexity of LSM trees are still on average O(N) where N = number of sorted string tables.
       So, we can use some kind of merging strategy to overwrite stale data and create a new sorted string tables, when we hit
       more than k amount of sst.

    * The main responsibility of SSTManager is handle creation of SST and merging of SST tables and tiered compaction
     where new sorted string tables live on top level in some kind of pyramid ranking. 

 */

public class SSTManager {

    private final AtomicInteger sstCounter = new AtomicInteger(0);

    private final Map<Integer, List<SSTable>> levels = new TreeMap<>();
    private final int MAX_SST_PER_LEVEL = 4;

    private final ExecutorService compactionExecutor; 

    public AtomicInteger getSSTCount() {
        return sstCounter;
    }

    public SSTManager() throws IOException{   
        repopulateIndexes();
        compactionExecutor = Executors.newSingleThreadExecutor();
    }

    /*
        * During startup or crash,to load all SST into memory and their indices to access, we need to somehow repopulate SST objects.
        But the main issue is that SST numbers can vary a lot because of merging logic.
        
        * Either, We can sort all folders in ascending order and start renaming them from sstable-001 to so on.. which is most viable option but also takes time.
        Or we can simply iterate through all SST and store maximum SST value that we have came across and set our sstCounter to max.
        I'm going with latter option to reduce time to repopulate SS tables.
    
    */
    
    public void repopulateIndexes() throws IOException {
        File dir = new File("SST");
        File[] folders = dir.listFiles(File::isDirectory);

        if (folders == null || folders.length == 0) {
            System.out.println("--- No SSTables found ---");
            return;
        }

        levels.clear();

        Arrays.sort(folders, Comparator.comparingInt(f -> {
            String name = f.getName();
            String[] vals = name.split("-");
            return Integer.parseInt(vals[1]);
        }));

        int maxId = 0;
        for (File folder : folders) {
            String name = folder.getName();
            String[] vals = name.split("-");
            if (vals.length < 2)
                 continue;
        
            int id = Integer.parseInt(vals[1]);
            maxId = Math.max(maxId, id);
        
            SSTable sst = new SSTable(folder);
            levels.computeIfAbsent(0, k -> new ArrayList<>()).add(sst);
        }
    
        sstCounter.set(maxId);

        System.out.println("Repopulated " + folders.length + " SSTables, counter at " + maxId);
    }

    public void flushToSSTable(TreeMap<String, String> memtable) throws IOException {
        int id = sstCounter.incrementAndGet();
        SSTable sst = new SSTable(id);
    
        for (Map.Entry<String, String> entry : memtable.entrySet()) {
            sst.put(entry.getKey(), entry.getValue());
        }
    
        sst.flush();
        sst.loadFromIndexFile();
    
        synchronized(this) {
            levels.computeIfAbsent(0, k -> new ArrayList<>()).add(sst);
        }
    
        compactionExecutor.submit(() -> {
            try {
                tieredCompaction();
            } catch (IOException e) {
                System.err.println("Compaction failed: " + e);
            }
        });
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

    /*
        * Let's say we have sstable-001, 002 ,003 , 004 and we need to merge them. sstable-004 contains most newest entries. So, we need to make sure
        only stale data gets overwritten by new entries and not vice versa. 

        After merging, older sstables gets slowly promoted or upgraded to next level because they are "old".

    */
    private synchronized void tieredCompaction() throws IOException {
        for (int level : new ArrayList<>(levels.keySet())) {
            List<SSTable> ssts = levels.get(level);

            while (ssts.size() >= MAX_SST_PER_LEVEL) {
                List<SSTable> toMerge = new ArrayList<>(ssts.subList(0, MAX_SST_PER_LEVEL));

                Collections.reverse(toMerge);

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

    /* 
         * to merge entries into one SST, I'm using k-way merge where we take data entries from each SST and if the key is found to be duplicate then
           newer SST value gets written into new SST because they are true or consistent value. if no conflict, then simply write all data into new SST.
    */

        PriorityQueue<Entry> pq = new PriorityQueue<>((e1, e2) -> {
        int cmp = e1.key.compareTo(e2.key); 
        if (cmp != 0)
             return cmp;

        // conflict of same keys ? write value of newer SST.
        return Integer.compare(e2.sstIndex, e1.sstIndex);
        });

        try {
            for (int i = 0; i < sstables.size(); i++) {
                SSTable sst = sstables.get(i);
                Path path = Paths.get("SST", sst.getSSTName(), sst.getSSTName() + ".data");
                DataInputStream dis = new DataInputStream(new BufferedInputStream(new FileInputStream(path.toFile())));
                streams.add(dis);

                try {
                    int len = dis.readInt();
                    byte[] buf = new byte[len];
                    dis.readFully(buf);

                    SSTWriteOuterClass.SSTWrite msg = SSTWriteOuterClass.SSTWrite.parseFrom(buf);
                    pq.add(new Entry(msg.getKey(), msg.getValue(), dis, i));  
                } catch (EOFException ignored) {
                    dis.close(); 
                }
            }

            String lastKey = null;

            while (!pq.isEmpty()) {
                Entry smallest = pq.poll();

                if(lastKey == null || !smallest.key.equals(lastKey)){
                    merged.put(smallest.key, smallest.value);
                    lastKey = smallest.key;
                }

                DataInputStream dis = smallest.stream;
                try {
                    int len = dis.readInt();
                    byte[] buf = new byte[len];
                    dis.readFully(buf);

                    SSTWriteOuterClass.SSTWrite msg = SSTWriteOuterClass.SSTWrite.parseFrom(buf);
                    pq.add(new Entry(msg.getKey(), msg.getValue(), dis, smallest.sstIndex));
                } catch (EOFException eof) {
                    dis.close(); 
                }
            }
        } finally {
            for (DataInputStream dis : streams) {
                try { dis.close(); } catch (IOException ignored) {}
            }
        }

        merged.flush();
        merged.loadFromIndexFile();
        return merged;
    }


    private static class Entry {
        String key, value;
        DataInputStream stream;
        int sstIndex;

        Entry(String key, String value, DataInputStream stream, int sstIndex) {
        this.key = key;
        this.value = value; 
        this.stream = stream;
        this.sstIndex = sstIndex; 
    }
    }
}
