package dev.bytekv.log;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.Serializable;
import java.time.Instant;
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

    private static volatile int serialNumber = 0;
    private static int lastTimestamp = 0;

    public static long returnSerialNumber() {
         return serialNumber; 
    }

    public static final ThreadLocal<CRC32> crcThreadLocal = ThreadLocal.withInitial(CRC32::new);

    public LogEntry(Operation operation, String key, String value) {
        this.operation = operation;
        this.key = key;
        this.value = value;
        this.timeStamp = Instant.now().toEpochMilli();
    }

    public static long getSerialNumber() {
        return serialNumber;
    }

    public LogEntryOuterClass.LogEntry toProto() {
        ++serialNumber;

        int delta = (int) (this.timeStamp - lastTimestamp);
        lastTimestamp = (int) this.timeStamp;

    /* * Each timestamp takes around 4 bytes of memory to write to a file. Since, I'm expecting this to be used for some kind of time series stuff.
    I'm using delta variant as timestamp values would always be monotonic (i.e same or increasing values). * So, timestamps can be written in only 1 byte
    because we are only storing differences compared to older timestamp. */

        byte[] recordBytes;
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            DataOutputStream dos = new DataOutputStream(baos);

            dos.writeInt(delta);
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
                .setTimestamp(delta)
                .setOp(protoOp)
                .setKey(this.key)
                .setValue(this.value != null ? this.value : "")
                .setChecksum((int) checkSum)
                .build();
    }
}
