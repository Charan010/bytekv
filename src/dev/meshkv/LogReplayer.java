package src.dev.meshkv;

//to-do: need to change parsing of data in the WAL file to replay

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;

public class LogReplayer {
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
                task.get();
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
}
