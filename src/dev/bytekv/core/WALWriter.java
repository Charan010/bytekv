package dev.bytekv.core;

import java.io.FileOutputStream;
import java.io.IOException;
import java.util.concurrent.LinkedBlockingQueue;
import dev.bytekv.proto.LogEntryOuterClass;

import java.io.File;
import java.util.concurrent.TimeUnit;
    
public class WALWriter{
    private final FileOutputStream fos;
    private final LinkedBlockingQueue<LogEntryOuterClass.LogEntry> queue;
    private final Thread writerThread;
    private volatile boolean running = true;
    private volatile Boolean backPressureOn;

    private int diskFlushLimit; 

    private static final int MAX_LOG_ENTRIES = 5;

    public WALWriter(String logFilePath, Boolean backPressureOn, int diskFlushLimit) throws IOException{
        this.fos = new FileOutputStream(new File(logFilePath), true);
        this.queue = new LinkedBlockingQueue<>(10_000);
        this.backPressureOn = backPressureOn;
        this.diskFlushLimit = diskFlushLimit;

        writerThread = new Thread(this::processLoop, "WAL-Writer");
        writerThread.start();

    }

    public void writeToLog(LogEntryOuterClass.LogEntry entry) throws IOException{
        //System.out.println("writing :/ " + entry.getKey() + " " + entry.getValue());
        if(!queue.offer(entry))
            throw new IOException("WAL queue overflow");
    }

    private void processLoop(){    
        try{
            int logCount = 0;

            while(running && (!queue.isEmpty() || backPressureOn)){
                LogEntryOuterClass.LogEntry entry = queue.poll(50, TimeUnit.MILLISECONDS);
                if(entry != null){
                    entry.writeDelimitedTo(fos);
                    ++logCount;
                }

                if(logCount >= diskFlushLimit){
                    
                    /*  forcefully flushing from kernel memory to disk for persistance. i'm choosing 5 as batch flushing to disk.
                        as forcefull flushing uses system calls which are expensive.
                     */

                    fos.flush();
                    fos.getFD().sync();
                    logCount = 0;
                }

            }

            fos.flush();
            fos.getFD().sync();
        
        }catch(Exception e){
            e.printStackTrace();
        }
    }

    public void shutDown() throws IOException, InterruptedException {
        running = false;
        writerThread.join();
        fos.close();
    }

}