package dev.bytekv.core.WAL;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.zip.CRC32;

/*

Current v1 Logging format looks like:

-------------------------------------------------------------
CRC32 checksum | Key length | value length | OP | Key | Value | 
---------------------------------------------------------------

OP - 0x00 - PUT
OP - 0x01 - DELETE

*/


public final class WalController {

    //private static final byte MAGIC = 0x01;

    private DataOutputStream out;
    private FileOutputStream fos;
    
    private final CRC32 crc = new CRC32();

    private int flushEvery = 15;
    private int syncEvery = 50;
    
    private int entriesSinceFlush = 0;
    private int entriesSinceSync  = 0;

    public WalController(String path, int flushEvery, int syncEvery) throws IOException {
        this.fos = new FileOutputStream(path, true);
        this.out = new DataOutputStream(new BufferedOutputStream(fos));
        this.flushEvery = flushEvery;
        this.syncEvery = syncEvery;
    }

    public WalController(String path) throws IOException{
        this.fos = new FileOutputStream(path, true);
        this.out = new DataOutputStream(new BufferedOutputStream(fos));
    }

    public void writeToFile(WalEntry e) throws IOException {
        
        byte[] keyBytes   = e.key.getBytes(StandardCharsets.UTF_8);
        byte[] valueBytes = e.value.getBytes(StandardCharsets.UTF_8);
        
        int keyLen = keyBytes.length;
        int valLen = valueBytes.length;


        crc.reset(); 
        crc.update(e.op);
        updateCrcInt(keyLen); 
        updateCrcInt(valLen); 
        crc.update(keyBytes);
        crc.update(valueBytes);
        
        int checksum = (int) crc.getValue();

        out.writeInt(checksum);
        out.writeInt(keyLen);
        out.writeInt(valLen);
        out.writeByte(e.op);
        
        out.write(keyBytes);
        out.write(valueBytes);

        entriesSinceFlush++;
        entriesSinceSync++;

        if (entriesSinceFlush >= flushEvery) {
            out.flush();

            /*
                flush() offers pushing data/heap memory to page cache where os gradually flushes
                 the dirty pages to disk for persistent data.

            */

            entriesSinceFlush = 0;
        }

        if (entriesSinceSync >= syncEvery) {
            if (entriesSinceFlush > 0) { 
                out.flush();
                entriesSinceFlush = 0;
            }

        /*
            fos.getFD().sync() is a blocking call which makes you sure to flush data forcefully onto disk
            and waits for an acknowledgement from DMA.
        */

            fos.getFD().sync();
            entriesSinceSync = 0;
        }
    }

    private void updateCrcInt(int i) {
        crc.update(i >>> 24);
        crc.update(i >>> 16);
        crc.update(i >>> 8);
        crc.update(i);
    }

    public void close() throws IOException {
        out.flush();
        fos.getFD().sync();
        out.close();
    }
}