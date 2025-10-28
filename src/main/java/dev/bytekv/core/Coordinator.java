package dev.bytekv.core;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.Lock;

import dev.bytekv.log.*;
import dev.bytekv.ttl.TTLManager;
import dev.bytekv.core.storage.*;

public class Coordinator implements KVStore{

    private final ExecutorService threadPool;
    private final int blockingQueueSize = 100;
    private final int threadPoolSize = 30;

    private final ReentrantReadWriteLock rw = new ReentrantReadWriteLock(); 
    private final Lock readerLock = rw.readLock();
    private final Lock writerLock = rw.writeLock();

    private LRUCache cache;
    private WALWriter writer;
    private MemTable memTable;
    private SSTManager sstManager;
    private TTLManager ttlManager;
    private LogRestorer logRestorer;

    Thread ttlManagerThread;

    CountDownLatch shutdownLatch = new CountDownLatch(3);
    private ConcurrentHashMap<String, StoreEntry> ttlEntries;

    public Coordinator(String logPath, int memTableLimit) throws IOException{

       String logFilePath = Paths.get(logPath, "master.log").toString();

      try{ sstManager = new SSTManager(); }

      catch(IOException e){
        throw new IOException(e);

      }

      try { writer = new WALWriter(logFilePath, shutdownLatch); }
      catch (IOException e) {
           throw new RuntimeException("Failed to initialize WAL", e); }

       memTable = new MemTable(this.sstManager, memTableLimit, writer);
       ttlEntries = new ConcurrentHashMap<>();

       cache = new LRUCache(2000);

       logRestorer = new LogRestorer(logPath, memTable);

    /*
        * Instead of scanning for expired TTL entries in memory. I'm using something called as lazy eviction.
        - Every 10 seconds, I'm randomly sampling 20 of the entries from all TLL entries. if atleast 75% of entries are
        expired from the sample. Then I'm triggering a full scan to evict any expired entries.

        * Tradeoffs:
        - Expired entries may live longer than their TTL.
        - Strict TTL guarantees are not enforced but data eventually gets consistent.
    */

        ttlManager = new TTLManager(ttlEntries,shutdownLatch);
        ttlManagerThread = new Thread(ttlManager);
        ttlManagerThread.setName("TTLManager thread");
        ttlManagerThread.setDaemon(true);
        ttlManagerThread.start();

       
        BlockingQueue<Runnable> taskQueue = new ArrayBlockingQueue<>(blockingQueueSize);

        /*

        ** Whenever threadpool gets exhausted,tasks gets stored in taskQueue, if taskQueue capacity reaches its maximum as well then,
            thread which picked up the current task would put in blocked state and busy waits until it founds place to put the task in 
            taskQueue.

        ** Writes are pretty fast in LSM based storage as most of persistant writes takes place in background so it's ok to block
        when taskQueue reaches its capacity instead of sending RejectedExecutionException.

        */

       threadPool = new ThreadPoolExecutor(
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
        logRestorer.replayLogs();
    }

    @Override
    public CompletableFuture<String> get(String key) {
        return CompletableFuture.supplyAsync(() -> {

            if(key == null)
                return null;

            readerLock.lock();
            try {
                String ans = cache.get(key);
                if (ans != null) return ans;
                return memTable.get(key); 
            } catch (IOException e) {
                throw new RuntimeException(e); 
            } finally {
                readerLock.unlock();
            }
        }, threadPool);
    }

    @Override
    public CompletableFuture<String> put(String key, String value) {
        return CompletableFuture.supplyAsync(() -> {
            
            try {
                LogEntry entry = new LogEntry(LogEntry.Operation.PUT, key, value);
                writer.writeToLog(entry.toProto()); 
            } catch (IOException e) {
                throw new RuntimeException("ERROR: log file not initialized properly", e);
            }

            writerLock.lock();
            try {
                cache.put(key, value);
                memTable.put(key, value);
                return "OK!";
            } catch (IOException e) {
                throw new RuntimeException("ERROR: failed to write to memTable", e);
            } finally {
                writerLock.unlock();
            }
        }, threadPool);
    }

    @Override
    public CompletableFuture<String> delete(String key) {
        return CompletableFuture.supplyAsync(() -> {
            if (key == null)
                return null;

            try {
                LogEntry logEntry = new LogEntry(LogEntry.Operation.DELETE, key, null);
                writer.writeToLog(logEntry.toProto()); 
            } catch (IOException e) {
                throw new RuntimeException("ERROR: log file not initialized properly", e);
            }

            writerLock.lock();
            try {
                memTable.delete(key);
                cache.delete(key);
                return "OK!";
            } catch (IOException e) {
                throw new RuntimeException("ERROR: failed to delete from memTable/cache", e);
            } finally {
                writerLock.unlock();
            }
        }, threadPool);
    }

     /*
        * I'm expecting TTL entries to be short lived. So I'm not logging TTL entries and instead storing them in memory in a 
          hashset.

        * Tradeoffs:
        - TTL entries are not persistent. So data can be lost when there is a crash of the database instance.
    */

    @Override
    public CompletableFuture<String> put(String key, String value, long expiryTime) {
        return CompletableFuture.supplyAsync(() -> {
            StoreEntry se = new StoreEntry(key, value, true, expiryTime);
            ttlEntries.put(key, se);
            return "OK!";
        }, threadPool);
    }

    @Override
    public CompletableFuture<String> getTTL(String key) {
        return CompletableFuture.supplyAsync(() -> {
            if (key == null)
                 return null;

            readerLock.lock();
            try {
            if (!ttlEntries.containsKey(key))
                 return null;

            StoreEntry obj = ttlEntries.get(key);
            if (System.currentTimeMillis() > obj.expiryTime) {
                ttlEntries.remove(key);
                return null;
            }
            return obj.value;
        } finally {
            readerLock.unlock();
        }
    }, threadPool);
    }

    @Override
    public void forceFlush(){
        try{ memTable.flush(); }
        
        catch(IOException e){
            System.out.println("Force flush failed:" + e.getMessage());
        }
    }

    
    public void shutDown() throws InterruptedException{
        try{
        ttlManager.stopIt();
        ttlManagerThread.interrupt();

        writer.stopIt();
        shutdownLatch.await();

        threadPool.shutdown();

        if (!threadPool.awaitTermination(60, TimeUnit.SECONDS))
            threadPool.shutdownNow(); 

        forceFlush();
        System.out.println("Coordinator shutdown complete.");
        
        }catch(InterruptedException e){
            throw new InterruptedException(e.getMessage());
        }
    
    }

}
