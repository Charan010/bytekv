package src;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
//import java.security.Key;
//import java.util.ArrayList;
import java.util.concurrent.*;

public class Main{

  public static boolean replayLogs(KeyValue masterNode) {

    /*Future<Boolean> is like a promise telling that each thread will return some boolean value 
        we are making sure all threads are done executing before going back.
    */
    List<Future<Boolean>> pendingTasks = new ArrayList<>();

    String filePath = "src/log/master.log";
    boolean hasData = false;

    try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
        String line;
        while ((line = reader.readLine()) != null) {
            hasData = true;

            String[] args = line.trim().split("\\s+");
            if (args.length > 3  || args.length < 2) {
                System.out.println("[WARN] Skipping invalid log line: " + line);
                continue;
            }

            String command = args[0].toUpperCase();
            String key = args[1];
            String value = (args.length == 3) ? args[2] : null;

            switch (command) {
                case "PUT" -> {
                    if (value == null) {
                        System.out.println("[WARN] Missing value for PUT: " + line);
                        continue;
                    }
                    pendingTasks.add(masterNode.addTask(key, value, false));
                }
                case "DELETE" -> pendingTasks.add(masterNode.deleteTask(key, false));
                default -> System.out.println("[WARN] Unknown command in log: " + line);
            }
        }

        if (!hasData) {
            System.out.println("[INFO] Empty log file.");
            return false;
        }

        System.out.println("Waiting for " + pendingTasks.size() + " tasks to finish...");
        for (Future<Boolean> task : pendingTasks) {
            try {
                task.get(3, TimeUnit.SECONDS);
            } catch (Exception e) {
                System.out.println("[ERROR] Task failed during replay: " + e.getMessage());
            }
        }

    } catch (IOException e) {
        System.out.println("[ERROR] Failed to read log file: " + e.getMessage());
        return false;
    }

    System.out.println("[INFO] Log replay completed.");
    return true;
}

    public static void basicTest(KeyValue masterNode) {
    List<Future<Boolean>> tasks = new ArrayList<>();

    try {
        for (int i = 1; i <= 20; i++) {
            tasks.add(masterNode.addTask("xyz" + i, "abc" + i, true));
        }

        for (int i = 10; i < 20; i++) {
            tasks.add(masterNode.deleteTask("xyz" + i, true));
        }

        for (Future<Boolean> task : tasks) {
            try {
                task.get();
            } catch (Exception e) {
                System.out.println("[ERROR] Task failed: " + e.getMessage());
            }
        }
    } catch (Exception e) {
        System.out.println("[ERROR] Unexpected error in basicTest: " + e.getMessage());
    }
    }

    public static void main(String[] args){

    final int threadPoolSize = 50;
    
    String logFilePath = "src/log/master.log";

    ConcurrentHashMap<String,String> hm = new ConcurrentHashMap<>();

    BufferedWriter bw = null;

    try{
        bw = new BufferedWriter(new FileWriter(logFilePath, true));
    }
    catch(IOException e){
        System.out.println("log file doesnt exist " + e.getMessage());
    }

    KeyValue masterNode = new KeyValue(hm, threadPoolSize,bw);
    
    masterNode.emptyKVStore();
    System.out.println("[INFO]: Starting Basic test to simulate");
    Main.basicTest(masterNode);

    System.out.println("[INFO]: Starting Log replaying");
    Main.replayLogs(masterNode);

    masterNode.getAllKV();
    masterNode.shutdown();

    try{
    bw.flush();
    bw.close();
    
    }catch(IOException e){
        System.out.println(e.getMessage());
    }

    }
}