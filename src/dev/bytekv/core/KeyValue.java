package dev.bytekv.core;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantLock;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.io.File;

import dev.bytekv.log.LogCompact;
import dev.bytekv.log.LogEntry;
import dev.bytekv.log.LogRestorer;
import dev.bytekv.ttl.TTLManager;

import dev.bytekv.proto.LogEntryOuterClass;


public class KeyValue implements KVStore{

    private final ExecutorService threadPool;
    
    private final ReentrantLock logLock = new ReentrantLock();
    
    public final String logFilePath;
    private final String logPath;
    private final int blockingQueueSize;

    private boolean logging = false;
    private boolean compactLogging = false;
    private Thread comThread;
    Thread ttlManager;
    public Set<StoreEntry> ttlEntries;
    SSTManager sstManager;
    private MemTable memTable;

    private Thread mergeThread;

    FileOutputStream fos;
    private ArrayList<LogEntryOuterClass.LogEntry> pendingWrites = new ArrayList<>();
    private static final int MAX_PENDING_WRITES = 10;

    public KeyValue(int threadPoolSize,int BlockingQueueSize ,String logFilePath, String logPath)
     {
        this.logFilePath = logFilePath;
        this.logPath = logPath;
        this.blockingQueueSize = BlockingQueueSize;
        this.ttlEntries = Collections.newSetFromMap(new ConcurrentHashMap<>());
        this.sstManager = new SSTManager();
        this.memTable = new MemTable(this.sstManager);

        /*
         running a background thread which would randomly take 20 kv pairs and check for ttl expiration.
         if 25% of them are expired, it would do a full scan.

        its pretty similar to lazy loading in redis,using this instead of full scanning every few seconds 
        would bottleneck performance

         */
        this.ttlManager = new Thread(new TTLManager(this));
        this.ttlManager.setName("TTLManager thread");
        this.ttlManager.setDaemon(true);
        this.ttlManager.start();


        this.mergeThread = new Thread(new Merger(this.sstManager));
        this.mergeThread.setName("SSTable Merger");
        this.mergeThread.setDaemon(true);
        this.mergeThread.start();
        
        try{
            setupFOS();
        }catch(IOException e){
            System.out.println("FOS ERROR :/ " + e.getMessage());
        }
        
        /* it can take up to x amount of tasks/threads when threadpool is out of threads. */

        BlockingQueue<Runnable> taskQueue = new ArrayBlockingQueue<>(this.blockingQueueSize);

        this.threadPool = new ThreadPoolExecutor(
            threadPoolSize,
            threadPoolSize,
            0L, TimeUnit.MILLISECONDS,
            taskQueue,
            Executors.defaultThreadFactory()
         );
        }

    @Override
    public void replayLogs(){
        LogRestorer lr = new LogRestorer(this);
        lr.replayLogs();
    }

    @Override
    public void logging(boolean flag){
        this.logging = flag;
    }

    @Override
    public Future<String> get(String key){
        return getTask(key);
    }

    @Override
    public Future<String> put(String key, String value) {
        return addTask(key, value);
    }

    @Override
    public Future<String> put(String key, String value, long ttlMillis) {
        long expiryTimestamp = System.currentTimeMillis() + ttlMillis;
        return addTask(key, value, expiryTimestamp);
    
    }
    @Override
    public Future<String> delete(String key) {
        try {
            return deleteTask(key); 
        } catch (Exception e) {
            System.out.println("Delete failed: " + e.getMessage());
            return CompletableFuture.completedFuture(null); 
        }
    }

     @Override
    public Future<String> delete(String key, long expiryTime) {
        try {
            return deleteTask(key, true, expiryTime);
        } catch (Exception e) {
            System.out.println("Delete failed: " + e.getMessage());
            return CompletableFuture.completedFuture(null); 
        }
    }


    @Override
    public void shutDown() {
        threadPool.shutdown();

        try {
            if (!threadPool.awaitTermination(5, TimeUnit.SECONDS))
                threadPool.shutdownNow();

            this.ttlManager.interrupt();

             if(this.mergeThread.isAlive())
                this.comThread.interrupt();

            if(this.comThread.isAlive())
                this.comThread.interrupt();

            if (fos != null) 
                fos.close();
        } catch (InterruptedException e) {
            threadPool.shutdownNow();
            Thread.currentThread().interrupt();

        } catch (IOException e) {
            System.out.println("Failed to close WAL: " + e.getMessage());
        }

        System.out.println("Shutting down bytekv...");
    }

    /*
        start a background thread which would check for number of entries in log.
        if it exceeds 1000 (hardcoded for now), it would trigger compact logging
        where duplicate or overwritten entries would be deleted to save space. 

     */

    @Override
    public void compactLogging(boolean flag){
        if(flag){
            this.compactLogging = true;
            comThread = new Thread(new LogCompact(this.logFilePath, this.logPath));
            comThread.setDaemon(true);
            comThread.setName("LogCompactor");
            comThread.start();

        }else{
            comThread.interrupt();
        }
    }

    public Future<String> getTask(String key){
            return threadPool.submit(() -> {
                return this.memTable.get(key);

        });
    }

    private void setupFOS() throws IOException{
        File logFile = new File(this.logFilePath);
        fos = new FileOutputStream(logFile, true);

    }


    /*
        flushing to disk batch wise as it performance would get pretty shit if i do flush to each write 
        as it uses syscalls and dma to write to disk. flushes 10 writes at a time.
     */
    private void flushToDisk() throws IOException {

        if (pendingWrites == null || pendingWrites.isEmpty())
            return;

        Iterator<LogEntryOuterClass.LogEntry> iterator = pendingWrites.iterator();

        while (iterator.hasNext()) {
            LogEntryOuterClass.LogEntry entry = iterator.next();
            try {
                entry.writeDelimitedTo(fos);
            } catch (IOException e) {
                throw new IOException("WAL file error: " + e.getMessage(), e);
            }
        }

        fos.flush();

        pendingWrites.clear();
    }

    private void writeToLog(LogEntryOuterClass.LogEntry entry) throws IOException {
    if (fos == null) {
        throw new IOException("WAL output stream not initialized.");
    }

    try {
        pendingWrites.add(entry);

        if (pendingWrites.size() > MAX_PENDING_WRITES) {
            flushToDisk(); // this will clear the list internally
        }

    } catch (IOException e) {
        throw new IOException("Failed to write log entry to WAL: " + e.getMessage(), e);
    }
}

  public Future<String> addTask(String key ,String value){
    return threadPool.submit(() -> {

        if(!this.logging){
            this.memTable.put(key,value);
            return "OK!";
        }

        logLock.lock();

        try{
            this.memTable.put(key,value);
            LogEntry logEntry = new LogEntry(LogEntry.Operation.PUT, key, value, false, -1);
            writeToLog(logEntry.toProto());
            return "OK!";

        }catch(IOException e){
            return "ERROR: log file not initialized properly";
        }
        finally{
            if(logLock.isHeldByCurrentThread())
            logLock.unlock();
        }
    });
  }


    public Future<String> addTask(String key ,String value, long expiryTime){
       return threadPool.submit(() -> {

        byte[] valueinBytes = value.getBytes(StandardCharsets.UTF_8);

        StoreEntry storeEntry = new StoreEntry(key, valueinBytes, true, expiryTime);

        if(!this.logging){
            ttlEntries.add(storeEntry);
            return "OK!";
        }

        logLock.lock();

        try{
            LogEntry logEntry = new LogEntry(LogEntry.Operation.PUT,key,value, true, expiryTime);
            writeToLog(logEntry.toProto());
            ttlEntries.add(storeEntry);
            return "OK!";

        }catch(IOException e){
            System.out.println("Failed to log entry for key: " + key + " error:" + e.getMessage());
            return "ERROR: log file not initialized properly";

        }
        finally{
            if(logLock.isHeldByCurrentThread())
                logLock.unlock();
        }
        });
    }


public Future<String> deleteTask(String key) {
    return threadPool.submit(() ->{
        if(!this.logging){
            return String.valueOf(this.memTable.delete(key));
        }
        logLock.lock();

        try{
            LogEntry logEntry = new LogEntry(LogEntry.Operation.DELETE, key, false , 0);
            writeToLog(logEntry.toProto());
            return "OK!";

        }catch(IOException error){
            return "ERROR: log file not initialized properly";

        }finally{
            if(logLock.isHeldByCurrentThread())
                logLock.unlock();
        }
    });
}


    /* overloading deleteTask with delayMs to delay deletes */
    public Future<String> deleteTask(String key,boolean isTTL, long delayMs){
        return threadPool.submit(() ->{

            if(memTable.get(key) == null)
                return "null";
            
            StoreEntry obj = new StoreEntry(key, null, isTTL, delayMs + System.currentTimeMillis());

            if(!this.logging){
                ttlEntries.add(obj);
                return "OK!";
            }

            logLock.lock();

            try{
                LogEntry logEntry = new LogEntry(LogEntry.Operation.DELETE, key, true , System.currentTimeMillis() + delayMs);
                writeToLog(logEntry.toProto());
                ttlEntries.add(obj);
                return "OK!";

            }catch(IOException error){
                return "ERROR: log file not initialized properly";

            }finally{
                if(logLock.isHeldByCurrentThread())
                    logLock.unlock();
            }
        });
    }

    @Override
    public void getAllKV(){
        System.out.println("----- KV STORE -----");

        for(Map.Entry<String, String> entry : this.memTable.buffer.entrySet()){
            System.out.println("key: " + entry.getKey() + " value: " + entry.getValue());
        }
    }
}