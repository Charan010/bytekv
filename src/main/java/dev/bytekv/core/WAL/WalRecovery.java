package dev.bytekv.core.WAL;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.FileInputStream;
import java.io.IOException;

import java.util.zip.CRC32;


public class WalRecovery {
    
    private DataInputStream in;
    private CRC32 crc32;

    public WalRecovery(String filePath) throws IOException{

        FileInputStream fis = new FileInputStream(filePath);
        this.in = new DataInputStream(new BufferedInputStream(fis));

        this.crc32 = new CRC32();
    }

    public void reconstructWalFile() throws IOException{

        try{

            while(true){

                int recievedCheckSum = in.readInt();
                int keyLength = in.readInt();
                int valueLength = in.readInt();

                byte op = in.readByte();

                byte[] keyBytes = new byte[keyLength];
                byte[] valueBytes = new byte[valueLength];

                in.readFully(keyBytes);
                in.readFully(valueBytes);

                crc32.reset();
                crc32.update(op);
                updateCrcInt(keyLength); 
                updateCrcInt(valueLength); 
                crc32.update(keyBytes);
                crc32.update(valueBytes);

                int computedCheckSum = (int)crc32.getValue();


                if(computedCheckSum != recievedCheckSum){
                    throw new IOException("WAL entry is corrupted. There is chance that whole file is corrupted");
                }
            }
        }
        catch(EOFException e){

        }
    }

    private void updateCrcInt(int i) {
        crc32.update(i >>> 24);
        crc32.update(i >>> 16);
        crc32.update(i >>> 8);
        crc32.update(i);
    }
}
