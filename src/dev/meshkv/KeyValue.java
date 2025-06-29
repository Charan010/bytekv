package dev.meshkv;

/* this "engine" could be thrown into a .jar file or container so servers can use it */
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantLock;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

class KeyValue{
    //changing from String to bytes[] for performance as String is bloated
    private final ConcurrentHashMap<String, byte[]> kvStore;
    private final ExecutorService threadPool;
    //will deal with it later. can be used for auditing for internal bugs or smtg.
    private final BlockingQueue<Runnable> deadLetterQueue = new LinkedBlockingDeque<>();
    private final ReentrantLock logLock = new ReentrantLock();
    private final int tpSize;

    FileOutputStream fos; 

    public KeyValue(ConcurrentHashMap<String, byte[]> hm, int threadPoolSize) {
        this.kvStore = hm;
        this.tpSize = threadPoolSize;
        try{
            setupFOS();
        }catch(IOException e){
            System.out.println(e.getMessage());
        }
        
        //can hold x threads in a queue when threadpool is exhausted.
        BlockingQueue<Runnable> taskQueue = new ArrayBlockingQueue<>(this.tpSize);

        this.threadPool = new ThreadPoolExecutor(
            threadPoolSize,
            threadPoolSize,
            0L, TimeUnit.MILLISECONDS,
            taskQueue,
            Executors.defaultThreadFactory()
        );
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
        try{
        fos = new FileOutputStream("log/master.log", true);

        }
        catch(FileNotFoundException error){
            throw new IOException("FOS file not found Error:" + error.getMessage());
        }
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
            System.out.println("Failed to log entry for key: " + key + "error:" + e.getMessage());
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

    public void shutdown() throws IOException {
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
            throw new IOException("Failed to close fos stream:" + e.getMessage());
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