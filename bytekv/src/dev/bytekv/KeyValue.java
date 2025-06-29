package dev.bytekv;

import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantLock;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.io.File;

public class KeyValue implements KVStore{
    //changing from String to bytes[] for performance as String is bloated
    private final ConcurrentHashMap<String, byte[]> kvStore;
    private final ExecutorService threadPool;
    //will deal with it later. can be used for auditing for internal bugs or smtg.
    private final BlockingQueue<Runnable> deadLetterQueue = new LinkedBlockingDeque<>();
    private final ReentrantLock logLock = new ReentrantLock();
    private final int tpSize;
    private final String logFilePath;
    private final String logPath;
    private final int blockingQueueSize;

    FileOutputStream fos; 


    public KeyValue(int threadPoolSize,int BlockingQueueSize ,String logFilePath, String logPath) {
        this.kvStore = new ConcurrentHashMap<>();
        this.tpSize = threadPoolSize;
        this.logFilePath = logFilePath;
        this.logPath = logPath;
        this.blockingQueueSize = BlockingQueueSize;
    
        try{
            setupFOS();
        }catch(IOException e){
            System.out.println("FOS ERROR :/ " + e.getMessage());
        }
        
        //can hold x threads in a queue when threadpool is exhausted.
        BlockingQueue<Runnable> taskQueue = new ArrayBlockingQueue<>(this.blockingQueueSize);

        this.threadPool = new ThreadPoolExecutor(
            threadPoolSize,
            threadPoolSize,
            0L, TimeUnit.MILLISECONDS,
            taskQueue,
            Executors.defaultThreadFactory()
        );
    }

       // Interface Methods
    @Override
    public void put(String key, String value) {
        addTask(key, value, true);
    }

    @Override
    public String get(String key) {
        try {
            return getTask(key).get(1, TimeUnit.SECONDS);
        } catch (Exception e) {
            return null;
        }
    }

    @Override
    public void delete(String key) {
        try {
            deleteTask(key, true).get(1, TimeUnit.SECONDS);
        } catch (Exception e) {
            System.out.println("Delete failed: " + e.getMessage());
        }
    }

    @Override
    public void shutDown() {
        threadPool.shutdown();

        try {
            if (!threadPool.awaitTermination(5, TimeUnit.SECONDS)) {
                threadPool.shutdownNow();
            }
            if (fos != null) fos.close();
        } catch (InterruptedException e) {
            threadPool.shutdownNow();
            Thread.currentThread().interrupt();
        } catch (IOException e) {
            System.out.println("Failed to close WAL: " + e.getMessage());
        }

        System.out.println("Shutting down MeshKV...");
    }

    @Override
    public void getAll() {
        System.out.println("\n--- Current Key-Value Store ---");
        for (Map.Entry<String, byte[]> entry : kvStore.entrySet()) {
            String value = new String(entry.getValue(), StandardCharsets.UTF_8);
            System.out.println(entry.getKey() + " -> " + value);
        }
    }

    @Override
    public void addCompactLogging() {
        Thread thread = new Thread(new LogCompact(this.logFilePath, this.logPath));
        thread.setDaemon(true);
        thread.setName("LogCompactor");
        thread.start();
    }

    public Future<String> getTask(String key){
            return threadPool.submit(() -> {
                byte[] byValue = kvStore.get(key);
                if(byValue == null)
                    return null;
                String value = new String(byValue, StandardCharsets.UTF_8);
                return value;

        });
    }

    private void setupFOS() throws IOException{
        File logFile = new File(this.logFilePath);

        fos = new FileOutputStream(logFile, true);
    }

    //writes to WAL with specific format
    private void writeToLog(LogEntry entry) throws IOException{
        if(fos == null){
            throw new IOException("WAL file output stream not initialized.");
        }

        try{
            byte[] logBytes = entry.toBytes();
            fos.write(logBytes);
        }catch(IOException e){
            throw new IOException("Failed to write log entry to WAL" + e.getMessage());
        }
    }


  public Future<Boolean> addTask(String key ,String value, boolean shouldLog){
    return threadPool.submit(() -> {
        byte[] valueinBytes = value.getBytes(StandardCharsets.UTF_8);

        if(!shouldLog){
            kvStore.put(key, valueinBytes);
            return true;
        }

        logLock.lock();

        try{
            LogEntry logEntry = new LogEntry(LogEntry.Operation.PUT,key,value);
            writeToLog(logEntry);
            kvStore.put(key, valueinBytes);
            return true;

        }catch(IOException e){
            System.out.println("Failed to log entry for key: " + key + " error:" + e.getMessage());
            return false;
        }
        finally{
            if(logLock.isHeldByCurrentThread())
            logLock.unlock();
        }
    });
  }

public Future<Boolean> deleteTask(String key, boolean shouldLog) {
    return threadPool.submit(() ->{
        if(!shouldLog){
            return !kvStore.remove(key).equals(null);
        }
        logLock.lock();

        try{
            LogEntry logEntry = new LogEntry(LogEntry.Operation.DELETE, key);
            writeToLog(logEntry);
            return true;

        }catch(IOException error){
            System.out.println("Failed to log entry for delete key:" + key + " error:" + error.getMessage());
            return false;
        }
    });
}

    public int getDLQSize(){
        return deadLetterQueue.size();
    }

    public void shutdown(){
        threadPool.shutdown();

        try {
            if (!threadPool.awaitTermination(5, TimeUnit.SECONDS)) {
                threadPool.shutdownNow();

                fos.close();
            }
         }catch (InterruptedException e) {
            threadPool.shutdownNow(); 
            Thread.currentThread().interrupt();

        }catch(IOException e){
            System.out.println("Failed to close fos stream:" + e.getMessage());
        }

        System.out.println("Shutting down gracefully :]...");

    }
    
    public void getAllKV(){
        System.out.println("\n--- Current Key-Value Store ---");

        for(Map.Entry<String,byte[]> entry :  kvStore.entrySet()){
            String value = new String(entry.getValue(), StandardCharsets.UTF_8);
            System.out.println(entry.getKey() + " -> " + value);
        }
    }

}