package dev.bytekv.core;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Queue;
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

import dev.bytekv.core.storage.MemTable;
import dev.bytekv.core.storage.SSTManager;
import dev.bytekv.core.storage.Merger;

//import dev.bytekv.core.pubsub.Publisher;

public class KeyValue implements KVStore{

    private final ExecutorService threadPool;
    
    private final ReentrantLock logLock = new ReentrantLock();
    
    public final String logFilePath;
    private final String logPath;
    private final int blockingQueueSize;

    private boolean logging = false;
    private boolean compactLogging = false;
    private Thread comThread;
    Thread ttlManagerThread;
    CountDownLatch shutdownLatch = new CountDownLatch(3);

    public boolean backPressureOn = false;

    //private HashMap<String,Publisher> publishersList;

    private LRUCache<String,String> lruCache;

    public Set<StoreEntry> ttlEntries;
    SSTManager sstManager;
    private MemTable memTable;

    private Thread mergeThread;
    private Merger merger;

    private TTLManager ttlManager;
    private LogCompact logCompact;

    FileOutputStream fos;
    private final Queue<LogEntryOuterClass.LogEntry> pendingWrites = new ConcurrentLinkedQueue<>();
    private static final int MAX_PENDING_WRITES = 5;

    public KeyValue(int threadPoolSize,int BlockingQueueSize ,String logFilePath, String logPath)
     {
        this.logFilePath = logFilePath;
        this.logPath = logPath;
        blockingQueueSize = BlockingQueueSize;
        ttlEntries = Collections.newSetFromMap(new ConcurrentHashMap<>());
        sstManager = new SSTManager();
        memTable = new MemTable(this.sstManager);
        lruCache = new LRUCache(30);

        //publishersList = new HashMap<>();

        backPressureOn = false;

        logCompact = new LogCompact(this.logFilePath, this.logPath, shutdownLatch, this);

        /*
         running a background thread which would randomly take 20 kv pairs and check for ttl expiration.
         if 25% of them are expired, it would do a full scan.

        its pretty similar to lazy loading in redis,using this instead of full scanning every few seconds 
        would bottleneck performance

         */

        merger = new Merger(this.sstManager, shutdownLatch);
        ttlManager = new TTLManager(this, shutdownLatch);

        ttlManagerThread = new Thread(ttlManager);
        ttlManagerThread.setName("TTLManager thread");
        ttlManagerThread.setDaemon(true);
        ttlManagerThread.start();


        this.mergeThread = new Thread(merger);
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
        public Future<String> getexp(String key){
            return threadPool.submit(() -> {

            Iterator<StoreEntry> itr = ttlEntries.iterator();
            while(itr.hasNext()){
                StoreEntry se = itr.next();
                if(se.key.equals(key))
                    return se.value;
            }

            return null;

        });
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
            return deleteTask(key, true, expiryTime + System.currentTimeMillis());
        } catch (Exception e) {
            System.out.println("Delete failed: " + e.getMessage());
            return CompletableFuture.completedFuture(null); 
        }
    }

    @Override
    public void flush(){
        try{
             flushToDisk();
             memTable.flush();

        }catch(IOException e){
            System.out.println(e.getMessage());
        }
    }

    @Override
    public void shutDown() {

        ttlManager.stopIt();
        merger.stopIt();

        if(comThread != null)
            logCompact.stopIt();

        try{
        shutdownLatch.await();
        memTable.flush();
        flushToDisk();

        if (fos != null) 
            fos.close();

        }catch(Exception e){
            System.out.println(e.getMessage());
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
            this.comThread = new Thread(logCompact);
            this.comThread.setDaemon(true);
            this.comThread.setName("LogCompactor");
            this.comThread.start();

        }else{
            if(this.comThread.isAlive() && this.comThread != null)
                logCompact.stopIt();
        }
    }

    public Future<String> getTask(String key){
            return threadPool.submit(() -> {

                String ans = lruCache.get(key);

                if(ans != null)
                    return ans;

                return memTable.get(key);

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

        if (pendingWrites.size() > MAX_PENDING_WRITES && !this.backPressureOn) {
            flushToDisk(); // this will clear the list internally
        }

    } catch (IOException e) {
        throw new IOException("Failed to write log entry to WAL: " + e.getMessage(), e);
    }
}

  public Future<String> addTask(String key ,String value){
    return threadPool.submit(() -> {

        if(!this.logging){
            lruCache.put(key, value);
            memTable.put(key,value);
            return "OK!";
        }

        logLock.lock();

        try{
            lruCache.put(key, value);
            memTable.put(key,value);
            LogEntry logEntry = new LogEntry(LogEntry.Operation.PUT, key, value, false, -1);
            writeToLog(logEntry.toProto());
            return "OK!";

        }catch(IOException e){
            return "ERROR: log file not initialized properly";
        }
        finally{
            logLock.unlock();
        }
    });
  }

    public Future<String> addTask(String key ,String value, long expiryTime){
       return threadPool.submit(() -> {

        StoreEntry storeEntry = new StoreEntry(key, value, true, expiryTime);

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
                logLock.unlock();
        }
        });
    }

public Future<String> deleteTask(String key) {
    return threadPool.submit(() ->{
        if(!this.logging){
            return memTable.delete(key);
        }
        logLock.lock();

        try{
            LogEntry logEntry = new LogEntry(LogEntry.Operation.DELETE, key, false , 0);
            writeToLog(logEntry.toProto());
            return "OK!";

        }catch(IOException error){
            return "ERROR: log file not initialized properly";

        }finally{
                logLock.unlock();
        }
    });
}


    /* overloading deleteTask with delayMs to delay deletes */
    public Future<String> deleteTask(String key,boolean isTTL, long expiryTime){
        return threadPool.submit(() ->{

            if(memTable.get(key) == null)
                return "null";
            
            StoreEntry obj = new StoreEntry(key, null, isTTL, expiryTime);

            if(!this.logging){
                ttlEntries.add(obj);
                return "OK!";
            }

            logLock.lock();

            try{
                LogEntry logEntry = new LogEntry(LogEntry.Operation.DELETE, key, true , expiryTime);
                writeToLog(logEntry.toProto());
                ttlEntries.add(obj);
                return "OK!";

            }catch(IOException error){
                return "ERROR: log file not initialized properly";

            }finally{
                    logLock.unlock();
            }
        });
    }

    /*public String createPublisher(String publisherName){
        return threadpool.submit(() -> {

            if(publishersList.containKey(publisherName)){
                return "ALREADY EXISTS!";
            }

            Publisher publisher = new Publisher(publisherName);
            publishersList.put(publisherName, publisher);

        });
    }
        */

}