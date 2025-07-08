package dev.bytekv.log;

import java.io.Serializable;
import java.time.Instant;

import dev.bytekv.proto.LogEntryOuterClass;


public class LogEntry implements Serializable{
    private final Operation operation;
    private final String key;
    private final String value;
    private final long timeStamp;
    private final long number;
    private final boolean isTTL;
    private final long expiryTime;

    public enum Operation {
        PUT, DELETE
    }

    private static volatile long serialNumber = 0;

    public static long returnSerialNumber() {
        return serialNumber;
    }

    private synchronized static long getNextSerialNumber() {
        return ++serialNumber;
    }

    public LogEntry(Operation operation, String key, String value, boolean isTTL, long expiryTime) {
        this.operation = operation;
        this.key = key;
        this.value = value;
        this.isTTL = isTTL;
        this.expiryTime = expiryTime;
        this.timeStamp = Instant.now().toEpochMilli();
        this.number = getNextSerialNumber();
    }

    public LogEntry(Operation operation, String key, boolean isTTL, long expiryTime) {
        this(operation, key, null, isTTL, expiryTime);
    }

    public LogEntryOuterClass.LogEntry toProto() {
        return LogEntryOuterClass.LogEntry.newBuilder()
            .setSerialNum(this.number)
            .setTimestamp(this.timeStamp)
            .setOperation(this.operation.name())
            .setKey(this.key)
            .setValue(this.value != null ? this.value : "")
            .setIsTTL(this.isTTL)
            .setExpiryTime(this.expiryTime)
            .build();
    }

    public byte[] toBytes() {
        return toProto().toByteArray();
    }
}