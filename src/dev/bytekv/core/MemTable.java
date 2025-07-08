package dev.bytekv.core;

/*

    All key&value pairs would be stored in treemap of max size 1024. when it exceeds, all the data will be flushed to a 
    sst(sorted string table) and index file would be created as accessing and searching through disk sucks.

    these sorted string tables would be merged when number of sst reaches 5 (hardcoded for now).
    Check out in Merger.java to change when to trigger SST merges.

 */


import java.io.IOException;
import java.util.List;
import java.util.TreeMap;

public class MemTable {

    private final SSTManager sstManager;
    public final TreeMap<String,String> buffer = new TreeMap<>();
    private final int MAX_FLUSH = 1024;
    private final String tombStone = "__<deleted>__";


    public MemTable(SSTManager sstManager) {
        this.sstManager = sstManager;
    }

    public synchronized void put(String key, String value) throws IOException{
        buffer.put(key, value);
        if(buffer.size() >= MAX_FLUSH)
            flush();
    }

    public boolean delete(String key) throws IOException{
        boolean flag = true;
        if(buffer.get(key) == null)
            flag = false;
       
        /* im using tombstone that is __<deleted>__ to represent that the certain value is deleted.
            as sst are immutable. we will be searching through new to old sst so we can handle deletes. 
        */    

        buffer.put(key, tombStone);
        if(buffer.size() >= MAX_FLUSH)
            flush();

        return flag;
    }

    public String get(String key) throws IOException {

        if(buffer.get(key) != null)
            return buffer.get(key);

        List<SSTable> allSSTables = sstManager.getSSTables();

        if(allSSTables.isEmpty())
            return null;

        for(SSTable table : allSSTables){
            if(table.get(key) != null)
                return table.get(key);
        }

        return null;
    }

     public void flush() throws IOException {
        TreeMap<String, String> temp = new TreeMap<>(this.buffer);
        sstManager.flushToSSTable(temp);
        
        buffer.clear();
    }
}
