package src.dev.meshkv;

/* this "engine" could be thrown into a .jar file or container so servers can use it */
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantLock;

import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.*;
import java.nio.charset.StandardCharsets;

class KeyValue{

    //changing from String to bytes[] for performance as String is bloated
    private final ConcurrentHashMap<ByteBuffer, byte[]> kvStore;
    private final ExecutorService threadPool;
    //will deal with it later. can be used for auditing for internal bugs or smtg.
    private final BlockingQueue<Runnable> deadLetterQueue = new LinkedBlockingDeque<>();
    private final ReentrantLock logLock = new ReentrantLock();
    private final int tpSize;

    BufferedWriter writer;
    FileOutputStream fos; 

    public KeyValue(ConcurrentHashMap<ByteBuffer, byte[]> hm, int threadPoolSize) {
        this.kvStore = hm;
        this.tpSize = threadPoolSize;

        try{
        this.writer = new BufferedWriter(
        new OutputStreamWriter(
            new FileOutputStream("src/log/master.log", true),
            StandardCharsets.UTF_8
            )
        );

        this.fos = new FileOutputStream("src/log/master.log", true);

        }catch(FileNotFoundException error){
            System.out.println("Error:" + error.getMessage());
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

    public Future<Boolean> getTask(String key){
            return threadPool.submit(() -> {
                ByteBuffer keyBuffer = ByteBuffer.wrap(key.getBytes(StandardCharsets.UTF_8));
                return kvStore.get(keyBuffer)!=null;
        });
    }

  public Future<Boolean> addTask(String key, String value, boolean shouldLog) {
    return threadPool.submit(() -> {
        ByteBuffer keyBuffer = ByteBuffer.wrap(key.getBytes(StandardCharsets.UTF_8));
        byte[] valueBytes = value.getBytes(StandardCharsets.UTF_8);

        /*  try to pick the lock after 2 seconds to stop getting fucking deadlock
            in log file otherwise fkk em :P */   
        boolean locked = logLock.tryLock(2, TimeUnit.SECONDS);
        if (!locked) {
            System.out.println("[WARN] Couldn't acquire log lock for PUT key: " + key);
            return false;
        }

            // Dont need to log when replaying from log file when on startup.
            if (shouldLog) {
                writer.write("PUT " + key + " " + value);
                writer.newLine();
                writer.flush();
            /*  forces to flush data to disk rather than being on os cache.
            little expensive but tradeoff is worth i guess.  */
                fos.getFD().sync(); 
            }

            kvStore.put(keyBuffer, valueBytes);
            logLock.unlock();
            return true;

    });
}

public Future<Boolean> deleteTask(String key, boolean logOrNot) {
    return threadPool.submit(() -> {
        boolean locked = logLock.tryLock(2, TimeUnit.SECONDS);
        if (!locked) {
            System.out.println("[WARN] Couldn't acquire log lock for DELETE key: " + key);
            return false;
        }

        try {
            ByteBuffer keyBuffer = ByteBuffer.wrap(key.getBytes(StandardCharsets.UTF_8));
            byte[] deleted = kvStore.remove(keyBuffer);

            if (deleted == null) {
                System.out.println("[INFO] Key not found for DELETE: " + key);
                return false;
            }

            if (logOrNot) {
                writer.write("DELETE " + key);
                writer.newLine();
                writer.flush();
                fos.getFD().sync(); 
            }
            return true;

        } catch (IOException e) {
            System.out.println("[ERROR] Failed to write DELETE log for key: " + key);
            return false;

        } finally {
            logLock.unlock();
        }
    });
}
    public void emptyKVStore(){
        this.kvStore.clear();
    }

    public int getDLQSize(){
        return deadLetterQueue.size();
    }

    public void shutdown() {
    threadPool.shutdown();

    try {
        if (!threadPool.awaitTermination(5, TimeUnit.SECONDS)) {
            threadPool.shutdownNow();
        }
    }catch (InterruptedException e) {
        threadPool.shutdownNow(); 
        Thread.currentThread().interrupt();
    }

    System.out.println("Shutting down gracefully :]...");
    }

    
    public void replayLogs(){
        //LogReplayer.replayLogs(this);
    }

    public void getAllKV(){

        if(kvStore.isEmpty()){
            System.out.println("KV is empty");
            return;
        }
        for (Map.Entry<ByteBuffer, byte[]> entry : kvStore.entrySet()) {
            ByteBuffer keyBuffer = entry.getKey();
            byte[] valueBytes = entry.getValue();

            String keyStr = new String(keyBuffer.array(), StandardCharsets.UTF_8);
            String valStr = new String(valueBytes, StandardCharsets.UTF_8);

            System.out.println(keyStr + " → " + valStr);
        }
    }

}