package dev.bytekv;

//simple example for using my key value store :P
public class MeshKVExample {
    public static void main(String[] args){

    int threadPoolSize = 100;
    String logFilePath = "master.log";
    String logPath = "log";
    int blockingQueueSize = 100;

    KVStore kv = new KeyValue(threadPoolSize, blockingQueueSize, logFilePath, logPath);

    kv.put("abc", "123");
    kv.put("lmao", "xoxox");
    kv.delete("xyz");

    kv.addCompactLogging();  

    }
}
