package dev.meshkv;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

public class Main{

    private static String delimiterExpression = "::|::";

    private static void replayLogs(KeyValue keyValue, String logFilePath) throws IOException,IllegalArgumentException{
        List<String> Logs = new ArrayList<>();
        Logs = null;

        try{
            Logs = Files.readAllLines(Paths.get(logFilePath));            
        }catch(IOException error){
            throw new IOException("Log file not found " + error.getMessage());
        }
    
    for(String log : Logs){
        if (log.trim().isEmpty()) continue;

        String args[] = log.split(Pattern.quote("::|::"), -1);

        if (args.length < 6) {
            System.out.println("[WARN]:bad logging structure" + log);
            continue;
        }

        // 2nd - op , 4 - key, 5- value
        String operation = args[2];
        String key = args[4];
        String value = args[5];

        if(key.contains(delimiterExpression) || value.contains(delimiterExpression))

        System.out.println("Replaying: key = " + key + ", value = " + value);


        if(operation.equals("PUT")){
            //System.out.println("PUT " + key + " " + value);
            keyValue.addTask(key, value, false);
        
        }else if(operation.equals("DELETE")){
            //System.out.println("DELETE " + key );
            keyValue.deleteTask(key, false);

        }else{
            System.out.println("[WARN]: invalid logging structure: " + log);
        }

    }
    }

    public static void main(String[] args){

        int threadPoolSize = 100; //hard coded for now :/
        ConcurrentHashMap<String,byte[]> hm = new ConcurrentHashMap<>();

        KeyValue keyValue = new KeyValue(hm ,threadPoolSize);

        try{
            Main.replayLogs(keyValue, "log/master.log");
        }catch(IOException error){
            System.out.println(error.getMessage());
        }

        keyValue.getAllKV();
        
    }
}
