package dev.bytekv.core.storage;

import java.io.*;
import java.nio.file.Paths;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Pattern;
import java.nio.file.Files;
import dev.bytekv.proto.SSTWriteOuterClass;

/*
    
    * When number of entries in memTable exceed, all data gets flushed to file in sorted order. The main reason to sort data is for range queries.
    To balance memory overhead and read latency, Instead of having indexes to all entries. I'm sparsely storing indexes of data every 4KB.


    * This way we can still acccess data pretty fast but we have to search nearest indexes and then do some kind of binary search to achieve almost
    O(logn) time complexity for reads.

    * Reads in LSM-based database still take around O(N) Where N = number of sorted string tables. I'm using bloom filters which are probabilistic data
    structures which can give if a data that we are querying exists or not.

    * Tradeoffs:
    - Sparsed indexes approach takes time if the MAX_SPARSE_INDEX is too long.
    - Bloom filters can still spit out false positives but is worth to have implement to decrease read latency as long we have good
    hash functions and more number of bitsets.
     
 */

public class SSTable {
    public LinkedHashMap<String, Long> indexMap;
    private String dataFile;
    private String indexFile;
    private String dirName;
    private String padded;

    private BloomFilter bloomFilter; 
    
    private long offSet = 0;


    /*
       * indexWriter is a wrapper to simple FileWriter which keeps a memory buffer to batchWrite to .index file.
       * dataSeeker uses RandomAccessFile which shines the best when you when you want to seek to specific offsets in a file.
       * dataOut is used to batch write to .data file which contains actual data
    */
    
    private BufferedWriter indexWriter;
    private RandomAccessFile dataSeeker;
    private DataOutputStream dataOut; 

    private static final String TOMBSTONE = "__<deleted>__";
    private static final String DELIMITER = "::||::";
    
    private long lastSeenIndexLength = 0;
    private static final int MAX_SPARSE_INDEX = 4096;

    public SSTable(int sstNumber) throws IOException {
        this.bloomFilter = new BloomFilter(10000);
        this.padded = String.format("%03d", sstNumber);
        this.dirName = "SST/sstable-" + padded;

        new File(dirName).mkdir();
            
        this.indexMap = new LinkedHashMap<>();
        this.indexFile = "sstable-" + padded + ".index";
        this.dataFile = "sstable-" + padded + ".data";

        String indexPath = Paths.get(dirName, this.indexFile).toString();
        String dataPath = Paths.get(dirName, this.dataFile).toString();

        new File(indexPath).createNewFile();
        new File(dataPath).createNewFile();

        indexWriter = new BufferedWriter(new FileWriter(indexPath));
        dataSeeker = new RandomAccessFile(dataPath, "r");
        dataOut = new DataOutputStream(new FileOutputStream(Paths.get(dirName, dataFile).toString(), true));

    }

    public SSTable(File folder) throws IOException {
        this.dirName = folder.getAbsolutePath();
        this.padded = folder.getName().split("-")[1];

        this.indexFile = "sstable-" + padded + ".index";
        this.dataFile = "sstable-" + padded + ".data";

        String indexPath = Paths.get(dirName, indexFile).toString();
        String dataPath = Paths.get(dirName, dataFile).toString();

        this.dataSeeker = new RandomAccessFile(dataPath, "r");
        this.indexMap = new LinkedHashMap<>();
        dataOut = new DataOutputStream(new FileOutputStream(Paths.get(dirName, dataFile).toString(), true));

        loadFromIndexFile();
    }

    public String getSSTName(){
        return "sstable-" + this.padded;
    }

    public void put(String key, String value) throws IOException {
        boolean isTombStone = TOMBSTONE.equals(key);

        long ts = System.currentTimeMillis();

       SSTWriteOuterClass.SSTWrite entry = SSTWriteOuterClass.SSTWrite.newBuilder()
        .setTimestamp(ts)
        .setKey(key)
        .setValue(value)
        .setIsTombstone(isTombStone)
        .build();

        byte[] bytes = entry.toByteArray();
        
        dataOut.writeInt(bytes.length);
        dataOut.write(bytes);

        if(lastSeenIndexLength > MAX_SPARSE_INDEX){
            indexWriter.write(key + DELIMITER + this.offSet + "\n");
            indexMap.put(key, offSet);
            lastSeenIndexLength = 0;
        }

        offSet += 4 + bytes.length;
        lastSeenIndexLength += (4 + bytes.length);
        bloomFilter.add(key);
    }

    void flush() {
        try {
            dataOut.flush();
            indexWriter.flush();
        }   catch(IOException e) {
            System.out.println(e.getMessage());
        }
    }

    public String get(String key) throws IOException {
        if (!bloomFilter.mightContain(key))
            return null;

        Entry<String, Long> nearest = null;
        for(Map.Entry<String, Long> e : indexMap.entrySet()){
            if(e.getKey().compareTo(key) <= 0 )
                nearest = e;
            else
                break;
        }

        long startOffSet = (nearest == null) ? 0 : nearest.getValue();
        dataSeeker.seek(startOffSet);

        while (dataSeeker.getFilePointer() < dataSeeker.length()) {
            int length = dataSeeker.readInt();
            byte[] buf = new byte[length];
            dataSeeker.readFully(buf);

            SSTWriteOuterClass.SSTWrite entry = SSTWriteOuterClass.SSTWrite.parseFrom(buf);
            String keyFromSST = entry.getKey();

            /*
                * Generally, key and value pair could be deleted easily when using some kind of in memory data structure. But when data is already persisted,
                to actually delete data, we need some kind of tombstone to mark that data as dead or should be deleted.

                * Since, to find data in SST, we iterate through latest sorted string tables to get consistent data . So ,we can ensure that data would be marked as 
                deleted and when SST compaction occurs, this data would be permanently deleted.
             
            */
            if (keyFromSST.equals(key) && entry.getIsTombstone())
                return null;

            /*
              * When comparing key to keyFromSST, if keyFromSST is bigger when compared lexicographically, then data that we need is not found because
                data is stored in sorted order.
            */
            if (keyFromSST.compareTo(key) > 0)
                break;

            if (keyFromSST.equals(key))
                return entry.getValue();
        }

        return null;
    }

    public void loadFromIndexFile() throws IOException {
        String path = Paths.get(dirName, indexFile).toString();
    
        List<String> lines = Files.readAllLines(Paths.get(path));

        if (lines == null || lines.isEmpty()) {
            System.out.println(this.dirName + " .index is empty");
            return;
        }
        for (String line : lines) {

        /*
            * Pattern.quote(DELIMITER) uses regex engine internally to escape DELIMITER sequence if key contains DELIMITER sequence.
            This way splitting of key,offset is done with proper edgecase.
        */
            String[] parts = line.split(Pattern.quote(DELIMITER), 2);
            if (parts.length == 2) {
            indexMap.put(parts[0], Long.parseLong(parts[1]));
            }
        }
    }

}