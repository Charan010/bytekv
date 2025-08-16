package dev.bytekv.core.storage;

import java.io.IOException;
import java.util.concurrent.*;

public class Merger implements Runnable {

    private final SSTManager sstManager;
    private volatile boolean running = true;
    private CountDownLatch shutdownLatch;

    public Merger(SSTManager sstManager, CountDownLatch shutdownLatch) {
        this.sstManager = sstManager;
        this.shutdownLatch = shutdownLatch;
    }

    public void stopIt(){
        running = false;
    }

   @Override
    public void run() {
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

        if(!running){
            shutdownLatch.countDown();
            return;
        }

        scheduler.scheduleAtFixedRate(() -> {
            /*if (sstManager.getSSTCount().get() > 5) {
                System.out.println("Merging SSTables...");

            try {
                sstManager.mergeSST();
            } catch (IOException e) {
                System.err.println("IO error during SST merge: " + e.getMessage());
            }

        }  */
        }, 0, 5, TimeUnit.MINUTES);
}
/* */ 

}
