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
        
        System.out.println("Triggered replaying log :P");
        String logFilepath = keyValue.logFilePath;

        try (FileInputStream fis = new FileInputStream(logFilepath)) {
            while (true) {

            /* im currently using protobuf to store log in binary format for faster writes and retrivals */
            LogEntryOuterClass.LogEntry entry = LogEntryOuterClass.LogEntry.parseDelimitedFrom(fis);
            if (entry == null)
                break;

            String key = entry.getKey();
            String value = entry.getValue();
            LogEntryOuterClass.LogEntry.Operation protoOp = entry.getOp();

            String operation = "";
            switch (protoOp) {
                case PUT:
                    operation = "PUT";
                    break;
                case DELETE:
                    operation = "DELETE";
                    break;
            }

            System.out.println("Operation: " + operation + " key: " + key + " value: " + value);
            
            if(operation.equals("PUT"))
                keyValue.memTable.put(key , value);
            
            else if(operation.equals("DELETE"))
                keyValue.memTable.delete(key);
                
        }
    } catch (IOException e) {
        System.out.println("Error replaying logs: " + e.getMessage());
    }
    }
}