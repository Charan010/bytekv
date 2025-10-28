package dev.bytekv.core;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;


/* 
   * The main issue with fixed size file chunking is that if current some bytes gets written at starting of file.
     then, whole fixed size would shift by number of bytes written and when it tries to check if this chunk exists already.
     It returns false. So instead of solely depending upon offset , we need to do sliding window efficiently based on
     content.
     
    So, I'm using rabin karp rolling algorithm here.


    initial hash = ((b0 * base^N + b1 * base^N + b2 *base^N + ...)%M

    new_hash = ((old_hash - b_out * Base ^ N)* base + b_in)%M

    take some pattern lets say if hash & mask = 0 then start chunking from that byte

*/


public class Chunker {

    private final int windowSize;
    private final int base;
    private final long M;
    private final int maskBits;
    private final long mask;
    private final long maxChunkSize;


    public Chunker(int windowSize , int base, long M ,int maskBits){
        this.windowSize = windowSize;
        this.base = base;
        this.M = M;
        this.maskBits = maskBits;
        this.mask = (1L << maskBits) - 1;
        this.maxChunkSize = 1L << 20; 
    }

    public int getScore(byte b){
        return b & 0xFF;
    }


    public interface ChunkHandler {
        void onChunk(byte[] chunkData, long startOffset, long endOffset);
    }


    public void chunkFile(String filePath, ChunkHandler handler) throws IOException{

        Path inputDir = Paths.get(filePath);
        Path outputDir = Paths.get("objects");

        Files.createDirectories(outputDir);

         try (InputStream in = new BufferedInputStream(Files.newInputStream(inputDir))) {

            Deque<Byte> window = new ArrayDeque<>();
            List<Byte> chunkBuf = new ArrayList<>();

            long hash = 0;
            long powBase = 1;
            for (int i = 0; i < windowSize - 1; ++i)
                powBase = (powBase * base) % M;
                
            int b;
            long offset = 0, startOffset = 0;
            int chunkId = 0;

            while (window.size() < windowSize && (b = in.read()) != -1) {
                byte bb = (byte) b;
                window.addLast(bb);
                chunkBuf.add(bb);
                hash = (hash * base + getScore(bb)) % M;
                offset++;
            }

            if (window.size() < windowSize) {
                System.out.println("file smaller than window size");
                return;
            }

            while ((b = in.read()) != -1) {
                byte newByte = (byte) b;
                byte outByte = window.removeFirst();
                window.addLast(newByte);
                chunkBuf.add(newByte);

                long val = (hash + M - (getScore(outByte) * powBase) % M) % M;
                hash = (val * base + getScore(newByte)) % M;
                offset++;

                boolean boundary = ((hash & mask) == 0) || (chunkBuf.size() >= maxChunkSize);

                if (boundary) {
                    handler.onChunk(chunkBuf.toArray(), startOffset, offset);
                    System.out.printf("chunk %d: %d bytes. Ending at %d%n",
                            chunkId - 1, (offset - startOffset), offset);
                    startOffset = offset;
                    chunkBuf.clear();
                }
            }

            if (!chunkBuf.isEmpty()) {
                handler.onChunk(chunkBuf.toArray(), startOffset, startOffset);
                System.out.printf("chunk %d: %d bytes (final)%n", chunkId - 1, (offset - startOffset));
            }
        }

    }

}
