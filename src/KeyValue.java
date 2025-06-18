package src;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantLock;
import java.io.BufferedWriter;
import java.io.IOException;

class KeyValue{

    private final ConcurrentHashMap<String, String> kvStore;
    private final ExecutorService threadPool;
    private final BlockingQueue<Runnable> deadLetterQueue = new LinkedBlockingDeque<>();
    private final ReentrantLock logLock = new ReentrantLock();
    private final int tpSize;

    BufferedWriter bufferWriter;

    public KeyValue(ConcurrentHashMap<String, String> hm, int threadPoolSize, BufferedWriter bw) {
    this.kvStore = hm;
    this.bufferWriter = bw;
    this.tpSize = threadPoolSize;

    BlockingQueue<Runnable> taskQueue = new ArrayBlockingQueue<>(100);

    this.threadPool = new ThreadPoolExecutor(
        threadPoolSize,
        threadPoolSize,
        0L, TimeUnit.MILLISECONDS,
        taskQueue,
        Executors.defaultThreadFactory(),

        (runnable, executor) -> {
                System.out.println("Blocking queue full. Will be waiting until empty. Current task queue size: " + taskQueue.size());
                // TO-DO: When task queue gets full. Im simply rejecting the task (like my crush) for now.
        }      
    );
}
    public Future<String> getTask(String key){
            return threadPool.submit(() -> {
                return kvStore.get(key);
        });

    }

    public Future<Boolean> addTask(String key, String value, boolean logOrNot) {
    return threadPool.submit(() -> {
        if (kvStore.containsKey(key)) {
            System.out.println("[INFO] Key already exists: " + key);
            return false;
        }

        boolean locked = logLock.tryLock(2, TimeUnit.SECONDS);
        if (!locked) {
            System.out.println("[WARN] Couldn't acquire log lock for PUT key: " + key);
            return false;
        }

        try {
            if (logOrNot) {
                bufferWriter.write("PUT " + key + " " + value);
                bufferWriter.newLine();
                bufferWriter.flush();
                System.out.println("[INFO] PUT logged: " + key);
            }
            kvStore.put(key, value);
            return true;

        }catch (IOException e){
            System.out.println("[ERROR] Failed to write PUT log for key: " + key);
            return false;
        } finally {
            logLock.unlock();
        }
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
            String deleted = kvStore.remove(key);
            if (deleted == null) {
                System.out.println("[INFO] Key not found for DELETE: " + key);
                return false;
            }

            if (logOrNot) {
                bufferWriter.write("DELETE " + key);
                bufferWriter.newLine();
                bufferWriter.flush();
                System.out.println("[INFO] DELETE logged: " + key);
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
    } catch (InterruptedException e) {
        threadPool.shutdownNow(); 
        Thread.currentThread().interrupt();
    }

    System.out.println("Shutting down gracefully😊...");

    }

    public void getAllKV(){

        if(kvStore.isEmpty()){
            System.out.println("KV is empty");
            return;
        }
        for (Map.Entry<String, String> entry : kvStore.entrySet()) {
        System.out.println("Key: " + entry.getKey() + ", Value: " + entry.getValue());
        }
    }

}