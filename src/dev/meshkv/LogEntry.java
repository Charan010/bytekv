package dev.meshkv;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.time.Instant;

class LogEntry implements Serializable{
    private final Operation operation;
    private final String key;
    private final String value;
    private final long timeStamp;
    private final long number;
    private final String delimiterFormat = "::|::";

    public static enum Operation{
        PUT, DELETE
    }

    private static volatile long serialNumber = 0;

    public static long returnSerialNumber(){
        return serialNumber;
    }

    private synchronized static long getNextSerialNumber(){
        return ++serialNumber;
    }

    public LogEntry(Operation operation , String key , String Value){
        this.operation = operation;
        this.key = key;
        this.value = Value;
        this.timeStamp = Instant.now().toEpochMilli();
        this.number = getNextSerialNumber();
    }

    public LogEntry(Operation operation, String key){
        this(operation ,key , null);
    }

    public byte[] toBytes(){
        StringBuilder sb = new StringBuilder();
        sb.append(this.number).append(delimiterFormat).append(this.timeStamp).append(delimiterFormat)
        .append(this.operation.name()).append(delimiterFormat).append(this.key.length()).append(delimiterFormat)
        .append(this.key).append(delimiterFormat);

        if(value == null)
            sb.append("0");
        else
            sb.append(this.value);

        sb.append("\n");

        return sb.toString().getBytes(StandardCharsets.UTF_8);
    }
}
