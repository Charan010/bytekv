package src.dev.meshkv;

import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentHashMap;

public class Main {
    public static void main(String[] args){

        int threadPoolSize = 100; //hard coded for now :/
        ConcurrentHashMap<ByteBuffer,byte[]> hm = new ConcurrentHashMap<>();
        KeyValue keyValue = new KeyValue(hm, threadPoolSize);

        //replay from log file on startup
        keyValue.replayLogs();

        LogJanitor logJanitor = new LogJanitor("src/dev/meshkv/log/master.log");
        Thread janitor = new Thread(logJanitor);

        janitor.setDaemon(true);
        janitor.start();
    }
}
