package dev.bytekv.core;

import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import dev.bytekv.core.storage.MemTable;
import dev.bytekv.core.storage.SSTManager;
import dev.bytekv.log.LogCompact;
import dev.bytekv.log.LogEntry;
import dev.bytekv.log.LogRestorer;
import dev.bytekv.ttl.TTLManager;

public class KeyValue implements KVStore{

    private final ExecutorService threadPool;
    
    private final ReentrantLock logLock = new ReentrantLock();
    
    public final String logFilePath;
    private final String logPath;
    private final int blockingQueueSize;

    private boolean isCompactLoggingOn = false;
    private Thread compactLoggingThread;
    Thread ttlManagerThread;
    CountDownLatch shutdownLatch = new CountDownLatch(3);

    public Boolean isBackPressureOn = false;

    private LRUCache lruCache;
    private LogRestorer lr;

    public ConcurrentHashMap<String, StoreEntry> ttlEntries;
    SSTManager sstManager;
    public MemTable memTable;

    private TTLManager ttlManager;
    private LogCompact logCompact;
    private WALWriter writer;

    public int memTableLimit;

    public KeyValue(int threadPoolSize,int BlockingQueueSize ,String logFilePath, String logPath, int memTableLimit) throws IOException
     {
        this.logFilePath = logFilePath;
        this.logPath = logPath;
        this.blockingQueueSize = BlockingQueueSize;
        this.memTableLimit = memTableLimit;

        ttlEntries = new ConcurrentHashMap<>();        
        try{
            sstManager = new SSTManager(); 
        }

        catch(IOException e){
            throw new IOException(e);
        }

        memTable = new MemTable(this.sstManager, memTableLimit);
        lruCache = new LRUCache(2000);
        
        isBackPressureOn = false;
        logCompact = new LogCompact(this.logFilePath, this.logPath, shutdownLatch, this, isBackPressureOn);
        
        try {
            writer = new WALWriter(logFilePath);

        } catch (IOException e) {
            throw new RuntimeException("Failed to initialize WAL", e);
        }

        /*
            * Instead of scanning for expired TTL entries in memory. I'm using something called as lazy eviction.
            - Every 10 seconds, I'm randomly sampling 20 of the entries from all TLL entries. if atleast 75% of entries are
              expired from the sample. Then I'm triggering a full scan to evict any expired entries.

            * Tradeoffs:
              - Expired entries may live longer than their TTL.
              - Strict TTL guarantees are not enforced but data eventually gets consistent.
         
        */

        ttlManager = new TTLManager(this, shutdownLatch);
        ttlManagerThread = new Thread(ttlManager);
        ttlManagerThread.setName("TTLManager thread");
        ttlManagerThread.setDaemon(true);
        ttlManagerThread.start();


        /*
            * When threads are exhausted from threadpool, tasks can be stored in this taskQueue.
              and when this taskQueue gets full as well, then it sends out a RejectedExecutionException.  
         
         */

        BlockingQueue<Runnable> taskQueue = new ArrayBlockingQueue<>(this.blockingQueueSize);

        this.lr = new LogRestorer(this);


       this.threadPool = new ThreadPoolExecutor(
            threadPoolSize,
            threadPoolSize,
            0L, TimeUnit.MILLISECONDS,
            taskQueue,
            Executors.defaultThreadFactory(),
            (r, executor) -> {
                try {
                    executor.getQueue().put(r);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        );
    }

    @Override
    public Future<String> getTTL(String key){
        return threadPool.submit(() -> {

        if(!ttlEntries.containsKey(key))
            return null;
        StoreEntry obj = ttlEntries.get(key);

        if(System.currentTimeMillis() > obj.expiryTime){
            ttlEntries.remove(key);
            return null;
        }
        return obj.value;
        });
    }
    
    @Override
    public void replayLogs(){
        lr.replayLogs();
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
    public void flush(){
        try{
             memTable.flush();

        }catch(IOException e){
            System.out.println(e.getMessage());
        }
    }

    @Override
    public void shutDown() {

        if(ttlManager != null)
            ttlManager.stopIt();
        
        if(compactLoggingThread != null)
            logCompact.stopIt();

        try{
        shutdownLatch.await();
        memTable.flush();
        writer.shutDown();

        }catch(Exception e){
            System.out.println(e.getMessage());
        }
        System.out.println("Shutting down bytekv...");
    
    }

    /*
        * To reduce the size of write ahead log(WAL) size, I'm creating a background thread which keeps track of how many entries in the log file.
        If the number of entries exceed 1000, log compaction is triggered where stale data are overwritten.
        
        * Tradeoffs:
        -  During startup, if log file is very huge , replaying log file still takes a lot of time.
        - Log compaction only works the best if there will be a lot of stale or overwritten data otherwise there is no use of compacting everytime.

     */

    @Override
    public void compactLogging(boolean flag){
        if(flag){
            isCompactLoggingOn = true;
            compactLoggingThread = new Thread(logCompact);
            compactLoggingThread.setDaemon(true);
            compactLoggingThread.setName("LogCompactor");
            compactLoggingThread.start();

        }else{
            if(compactLoggingThread.isAlive() && compactLoggingThread != null)
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

    public Future<String> addTask(String key ,String value){
        return threadPool.submit(() -> {
            logLock.lock();

            try{
                lruCache.put(key, value);
                memTable.put(key,value);
                LogEntry entry = new LogEntry(LogEntry.Operation.PUT, key, value);
                writer.writeToLog(entry.toProto());
                return "OK!";

            }catch(IOException e){
                return "ERROR: log file not initialized properly";
            }
            finally{
                logLock.unlock();
            }
        });
    }


    /*
        * I'm expecting TTL entries to be short lived. So I'm not logging TTL entries and instead storing them in memory in a 
          hashset.

        * Tradeoffs:
        - TTL entries are not persistent. So data can be lost when there is a crash of the database instance.
    */

    public Future<String> addTask(String key ,String value, long expiryTime){
       return threadPool.submit(() -> {
            StoreEntry se = new StoreEntry(key , value, true, expiryTime);
            ttlEntries.put(key ,se);
            return "OK!";
        });
    }

    public Future<String> deleteTask(String key) {
        return threadPool.submit(() ->{
            logLock.lock();

            try{
                LogEntry logEntry = new LogEntry(LogEntry.Operation.DELETE, key, null);
                writer.writeToLog(logEntry.toProto());
                memTable.delete(key);
                return "OK!";

            }catch(IOException error){
                return "ERROR: log file not initialized properly";

            }finally{
                logLock.unlock();
            }
        });
    }

}