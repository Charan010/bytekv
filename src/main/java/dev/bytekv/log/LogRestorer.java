package dev.bytekv.log;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.zip.CRC32;

import dev.bytekv.proto.LogEntryOuterClass;
import dev.bytekv.core.KeyValue;
import dev.bytekv.core.storage.MemTable;

public class LogRestorer{
    
    String logPath;
    MemTable memTable;

    public LogRestorer(String logPath, MemTable memTable){
        this.logPath = logPath;
        this.memTable = memTable;
    }

   /*  public static boolean verifyEntry(LogEntryOuterClass.LogEntry entry) {
    try {

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);

        dos.writeInt(entry.getTimestamp());
        dos.writeUTF(entry.getKey());
        dos.writeUTF(entry.getValue());
        dos.flush();

        byte[] recordBytes = baos.toByteArray();

        CRC32 crc = LogEntry.crcThreadLocal.get();
        crc.reset();
        crc.update(recordBytes);

        return (int) crc.getValue() == entry.getChecksum();
        } catch (Exception e) {
            return false;
        }
} */

    public void replayLogs() {
        
        System.out.println("Triggered replaying log :P");
        String logFilepath = Paths.get(logPath, "master.log").toString();

        try (FileInputStream fis = new FileInputStream(logFilepath)) {
            while (true) {

            /* im currently using protobuf to store log in binary format for faster writes and retrivals */
            LogEntryOuterClass.LogEntry entry = LogEntryOuterClass.LogEntry.parseDelimitedFrom(fis);
            
            if (entry == null)
                break;

            /*if (!verifyEntry(entry)) {
                System.err.println("Corrupted WAL entry detected! Stopping replay.");
                break; 
            }*/

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
                memTable.put(key , value);
            
            else if(operation.equals("DELETE"))
                memTable.delete(key);
                
        }
    } catch (IOException e) {
        System.out.println("Error replaying logs: " + e.getMessage());
    }
    }
}