package dev.bytekv.log;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.Serializable;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.CRC32;

import dev.bytekv.proto.LogEntryOuterClass;

public class LogEntry implements Serializable {

    private final Operation operation;
    private final String key;
    private final String value;
    private final long timeStamp;
    private long checkSum;

    public enum Operation {
        PUT, DELETE
    }

    private static final AtomicLong serialNumber = new AtomicLong(0);
    private static volatile long lastTimestamp = 0;

    public static final ThreadLocal<CRC32> crcThreadLocal = ThreadLocal.withInitial(CRC32::new);

    public LogEntry(Operation operation, String key, String value) {
        this.operation = operation;
        this.key = key;
        this.value = value;
        this.timeStamp = Instant.now().toEpochMilli();
    }

    public long getSerialNumber() {
        return serialNumber.get();
    }

    public LogEntryOuterClass.LogEntry toProto() {

        long delta = this.timeStamp - lastTimestamp;
        lastTimestamp = this.timeStamp;
        serialNumber.incrementAndGet();

        byte[] recordBytes;
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            DataOutputStream dos = new DataOutputStream(baos);

            dos.writeLong(delta); 
            dos.writeUTF(this.key);
            dos.writeUTF(this.value != null ? this.value : "");
            dos.flush();

            recordBytes = baos.toByteArray();
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize LogEntry for CRC", e);
        }

        CRC32 crc = crcThreadLocal.get();
        crc.reset();
        crc.update(recordBytes);
        checkSum = crc.getValue(); 

        LogEntryOuterClass.LogEntry.Operation protoOp =
                this.operation == Operation.PUT
                        ? LogEntryOuterClass.LogEntry.Operation.PUT
                        : LogEntryOuterClass.LogEntry.Operation.DELETE;

        return LogEntryOuterClass.LogEntry.newBuilder()
                .setTimestamp((int)delta) 
                .setOp(protoOp)
                .setKey(this.key)
                .setValue(this.value != null ? this.value : "")
                //.setChecksum(checkSum)
                .build();
    }
}
