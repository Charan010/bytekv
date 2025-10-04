package dev.bytekv.core.storage;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Pattern;
import java.nio.file.Files;
import dev.bytekv.proto.SSTWriteOuterClass;

/*
    keys and values are flushed into instance of this. it will load indices of each pair from .index file
    so we dont need to search through files each time to "get".

 */

public class SSTable {
    public LinkedHashMap<String, Long> indexMap;
    private String dataFile;
    private String indexFile;
    private String dirName;
    private String padded;

    private BloomFilter bloomFilter; 
    
    private long offSet = 0;

    private BufferedWriter indexWriter;
    private BufferedWriter dataWriter;
    private RandomAccessFile dataSeeker;
    private DataOutputStream dataOut; 


    private static final String TOMBSTONE = "__<deleted>__";
    private static final String DELIMITER = "::||::";
    
    private long lastSeenIndexLength = 0;
    //instead of keeping indices for all entries, im sparsely storing indexes for every 4KB.
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

        this.indexWriter = new BufferedWriter(new FileWriter(indexPath));
        this.dataWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(dataPath)));
        this.dataSeeker = new RandomAccessFile(dataPath, "r");
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

            // tombstone check
            if (keyFromSST.equals(key) && entry.getIsTombstone())
                return null;

    
            //since, data is sorted , you can check by lexicographical manner and exit if we exceed.
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
            String[] parts = line.split(Pattern.quote(DELIMITER), 2);
            if (parts.length == 2) {
            indexMap.put(parts[0], Long.parseLong(parts[1]));
            }
        }
    }

}