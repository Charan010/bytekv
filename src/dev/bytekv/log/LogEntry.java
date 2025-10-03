package dev.bytekv.log;

import java.io.Serializable;
import java.time.Instant;

import dev.bytekv.proto.LogEntryOuterClass;

public class LogEntry implements Serializable{
    private final Operation operation;
    private final String key;
    private final String value;
    private final long timeStamp;

    public enum Operation {
        PUT, DELETE
    }

    private static volatile int serialNumber = 0;
    private static int lastTimestamp = 0; 

   public static long returnSerialNumber() {
        return serialNumber;
    }

   /* private synchronized static long getNextSerialNumber() {
        return ++serialNumber;
    } */

    public LogEntry(Operation operation, String key, String value) {
        this.operation = operation;
        this.key = key;
        this.value = value;
        this.timeStamp = Instant.now().toEpochMilli();
        //this.number = getNextSerialNumber();
    }

    public LogEntryOuterClass.LogEntry toProto() {

        ++serialNumber;

        int delta = (int)(this.timeStamp - lastTimestamp);  
        lastTimestamp = (int) this.timeStamp;          

        LogEntryOuterClass.LogEntry.Operation protoOp =
            this.operation == Operation.PUT
                ? LogEntryOuterClass.LogEntry.Operation.PUT
                : LogEntryOuterClass.LogEntry.Operation.DELETE;

        return LogEntryOuterClass.LogEntry.newBuilder()
            .setTimestamp(delta)
            .setOp(protoOp)
            .setKey(this.key)
            .setValue(this.value != null ? this.value : "")
            .build();
    }
}