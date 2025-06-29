package dev.meshkv;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.*;
import java.util.regex.Pattern;


class LogFilter{
    String id;
    String timestamp;
    String line;
    String operation;
    String key;
    String value;

    private static final String delimiterFormat = "::|::";

    public LogFilter(String line){
        String[] args = line.split(Pattern.quote(delimiterFormat), -1);

        if (args.length < 6) {
            throw new IllegalArgumentException(line);
        }
        this.id = args[0];
        this.timestamp = args[1];
        this.operation = args[2].trim();
        this.key = args[3].trim();
        this.value = args[5];
    }
}

class LogCompact{
    private static String logPath;
    private static String logFilePath;

    public LogCompact(String LogPath, String LogFilePath) {
        logPath = LogPath;
        logFilePath = LogFilePath;
    }

    private List<String> returnAllLogs() throws IOException {
        try {
            return Files.readAllLines(Paths.get(logFilePath));
        } catch (IOException e) {
            throw new IOException("Log file path error: " + e.getMessage());
        }
    }

    public void compactLog(){
        List<String> allLogs;

        try{
            allLogs = returnAllLogs();
        }catch(IOException e){
            System.out.println(e.getMessage());
            return;
        }

        System.out.println("---- Starting Log compaction -----");

        Map<String,Integer> lastestOps = new HashMap<>();
        Set<String> deletedKeys = new HashSet<>();

        for(int i = 0 ; i < allLogs.size(); ++i){
            String log = allLogs.get(i);
            LogFilter lf = null;
        
            try{
                lf = new LogFilter(log);
            }catch(IllegalArgumentException e){
                System.out.println("Bad structure log :/ " + e.getMessage());
            }
            if(lf.operation.equals("DELETE")){
                lastestOps.remove(lf.key);
                deletedKeys.add(lf.key);
            }else if(lf.operation.equals("PUT")){
                lastestOps.put(lf.key, i);
                deletedKeys.remove(lf.key);
            }
        }

        List<String> compactedLog = new ArrayList<>();
        Set<Integer> indicesToKeep = new HashSet<>(lastestOps.values());

        for (int i = 0; i < allLogs.size(); i++) {
            if (indicesToKeep.contains(i)) {
                compactedLog.add(allLogs.get(i));
            }
        }
        

        Path compactedPath = Paths.get(logPath + "/compacted.log"); 
        Path originalPath = Paths.get(logFilePath);

        try{
            Files.write(compactedPath, compactedLog);
            Files.move(compactedPath, originalPath, StandardCopyOption.REPLACE_EXISTING);
            Files.deleteIfExists(compactedPath);
            System.out.println("---- Log compaction is succesful -----");
        
        }catch(IOException error){
            System.out.println("[ERROR] Failed to write compacted log " + error.getMessage());
        }
    }
}