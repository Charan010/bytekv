package dev.bytekv.core;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.nio.file.Files;

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

    
    public SSTable(int sstNumber) throws IOException {
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

    public String getSSTName(){
        return "sstable-" + this.padded;
    }

    public void put(String key, String value) throws IOException {
        String line = key + "," + value + "\n";
        this.indexMap.put(key, this.offSet);
        this.dataWriter.write(line);
        this.indexWriter.write(key + ":" + this.offSet + "\n");
        this.offSet += line.getBytes(StandardCharsets.UTF_8).length;

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

        if(!bloomFilter.mightContain(key))
            return null;

        Long offSet = this.indexMap.get(key);
        if (offSet == null) return null;

        dataSeeker.seek(offSet);
        String line = dataSeeker.readLine();

        if (line == null)
            return null;
        String[] parts = line.split(",", 2);

        if (parts.length < 2)
            return null;

        if (parts[1].equals("__<deleted>__"))
            return null;

        return parts[1];
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
