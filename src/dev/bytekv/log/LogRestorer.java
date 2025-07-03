package dev.bytekv.log;

import java.io.FileInputStream;
import java.io.IOException;

import dev.bytekv.proto.LogEntryOuterClass;
import dev.bytekv.core.KeyValue;

public class LogRestorer{
    KeyValue keyValue;

    public LogRestorer(KeyValue keyValue){
        this.keyValue = keyValue;
    }

    public void replayLogs() {
        String logFilepath = keyValue.logFilePath;

        try (FileInputStream fis = new FileInputStream(logFilepath)) {
            while (true) {

            /* im currently using protobuf to store log in binary format for faster writes and retrivals */
            LogEntryOuterClass.LogEntry entry = LogEntryOuterClass.LogEntry.parseDelimitedFrom(fis);
            if (entry == null) break;

            String key = entry.getKey();
            String value = entry.getValue();
            String operation = entry.getOperation();
            boolean isTTL = entry.getIsTTL();
            long expiryTime = entry.getExpiryTime();

            if (!isTTL) {
                if (operation.equals("PUT"))
                    keyValue.put(key, value);

                else if (operation.equals("DELETE"))
                    keyValue.delete(key);
            }else {
               
                if (System.currentTimeMillis() > expiryTime)
                    keyValue.delete(key); 
                
                else {
                    if (operation.equals("PUT"))
                        keyValue.put(key, value);
                }
            }
        }
    } catch (IOException e) {
        System.out.println("Error replaying logs: " + e.getMessage());
    }
    }
}
