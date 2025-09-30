package dev.bytekv.core.storage;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.nio.file.Files;
import dev.bytekv.proto.SSTWriteOuterClass;
import java.util.regex.Pattern;

/*
    keys and values are flushed into instance of this. it will load indices of each pair from .index file
    so we dont need to search through files each time to "get".

 */

public class SSTable {
    public Map<String, Long> indexMap;
    private String dataFile;
    private String indexFile;
    private String dirName;
    private String padded;

    private BloomFilter bloomFilter; 
    
    private long offSet = 0;

    private BufferedWriter indexWriter;
    private BufferedWriter dataWriter;
    private RandomAccessFile dataSeeker;

    private static final String TOMBSTONE = "__<deleted>__";
    private static final String DELIMITER = "::|::";
    
    public SSTable(int sstNumber) throws IOException {
        this.bloomFilter = new BloomFilter(10000);
        this.padded = String.format("%03d", sstNumber);
        this.dirName = "SST/sstable-" + padded;

        new File(dirName).mkdir();
            
        this.indexMap = new HashMap<>();
        this.indexFile = "sstable-" + padded + ".index";
        this.dataFile = "sstable-" + padded + ".data";

        String indexPath = Paths.get(dirName, this.indexFile).toString();
        String dataPath = Paths.get(dirName, this.dataFile).toString();


        new File(indexPath).createNewFile();
        new File(dataPath).createNewFile();

        this.indexWriter = new BufferedWriter(new FileWriter(indexPath));
        this.dataWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(dataPath)));
        this.dataSeeker = new RandomAccessFile(dataPath, "r");
    }

    public SSTable(File folder) throws IOException {
        this.dirName = folder.getAbsolutePath();
        this.padded = folder.getName().split("-")[1];

        this.indexFile = "sstable-" + padded + ".index";
        this.dataFile = "sstable-" + padded + ".data";

        String indexPath = Paths.get(dirName, indexFile).toString();
        String dataPath = Paths.get(dirName, dataFile).toString();

        this.dataSeeker = new RandomAccessFile(dataPath, "r");
        this.indexMap = new HashMap<>();

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
        indexMap.put(key, offSet);

        DataOutputStream dos = new DataOutputStream(new FileOutputStream(
        Paths.get(dirName, dataFile).toString(), true));
        dos.writeInt(bytes.length); // length prefix
        dos.write(bytes);
        dos.close();

        this.indexWriter.write(key + ":" + this.offSet + "\n");

        this.offSet += 4 + bytes.length;

        bloomFilter.add(key);
    }

    void flush(){
        try{
        this.dataWriter.flush();
        this.indexWriter.flush();

        }catch(IOException e){
            System.out.println(e.getMessage());
        }
    }

    public String get(String key) throws IOException {
        if (!bloomFilter.mightContain(key))
            return null;

        Long offset = this.indexMap.get(key);
        if(offset == null)
             return null;

        dataSeeker.seek(offset);

        int length = dataSeeker.readInt();
        byte[] buf = new byte[length];
        dataSeeker.readFully(buf);

        SSTWriteOuterClass.SSTWrite entry = SSTWriteOuterClass.SSTWrite.parseFrom(buf);

        if(entry.getIsTombstone())
             return null;

        return entry.getValue();
    }

    public void loadFromIndexFile() throws IOException {
        String path = Paths.get(this.dirName, this.indexFile).toString();
    
        List<String> lines = Files.readAllLines(Paths.get(path));

        if (lines == null || lines.isEmpty()) {
            System.out.println(this.dirName + " .index is empty");
            return;
        }

        for (String line : lines) {
            String[] parts = line.split(":", 2);
            if (parts.length == 2) {
            indexMap.put(parts[0], Long.parseLong(parts[1]));
            }
        }
    }

}