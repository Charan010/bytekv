import java.util.List;
import java.util.concurrent.*;
import java.util.ArrayList;

class KeyValue{

    private final ConcurrentHashMap<String, String> hm;
    private final ExecutorService threadPool;
    private final BlockingQueue<Runnable> deadLetterQueue = new LinkedBlockingDeque<>();

    public KeyValue(ConcurrentHashMap<String, String> hm, int threadPoolSize) {
    this.hm = hm;

    this.threadPool = new ThreadPoolExecutor(
        threadPoolSize,
        threadPoolSize,
        0L, TimeUnit.MILLISECONDS,
        new ArrayBlockingQueue<>(1),

        Executors.defaultThreadFactory(),
        (runnable, executor) -> {
            System.out.println("Task rejected. Pushing it into DLQ.");
            deadLetterQueue.offer(runnable);

        }
    );
}

    public Future<Boolean> getTask(String key){
        return threadPool.submit(() -> {

        try {
            Thread.sleep(2000); 
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

            Thread.sleep(3000);
            return hm.get(key)!=null;

        });
    }


    public Future<Boolean> addTask(String key , String value){
        try{
            return threadPool.submit(() -> {
        
        try {
            Thread.sleep(2000); 
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
            if(hm.containsKey(key))
                return false;


            hm.put(key, value);
            return true;
        });


    }catch(RejectedExecutionException e){
        System.out.println("Rejected a task pushed it into dlq");
    }
        return null;

    }

    public int getDLQSize(){
        return deadLetterQueue.size();
    }
}

public class Main{
    public static void main(String[] args){

    final int threadPoolSize = 3;    

    List<Future<Boolean>> results = new ArrayList<>();

    ConcurrentHashMap<String,String> hm = new ConcurrentHashMap<>();

    KeyValue obj = new KeyValue(hm, threadPoolSize);    
    
    try {
    for (int i = 0; i < 10; ++i) {
        Future<Boolean> res = obj.addTask("abc" + i, "xyz" + i);
        if (res != null)
            results.add(res); 
    }

    int i = 1;
    for (Future<Boolean> res : results) {
        System.out.println("Task " + i + " is Running: " + res.get()); 
        ++i;
    }
    System.out.println("DLQ size: " + obj.getDLQSize());

    }catch (Exception e){
    System.out.println("Error: " + e.getMessage());
    
    }

    }
}