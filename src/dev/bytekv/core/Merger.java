package dev.bytekv.core;

import java.io.IOException;
import java.util.concurrent.*;

public class Merger implements Runnable {

    private final SSTManager sstManager;

    public Merger(SSTManager sstManager) {
        this.sstManager = sstManager;
    }

   @Override
    public void run() {
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

        scheduler.scheduleAtFixedRate(() -> {
            if (SSTManager.getSSTCount() > 5) {
                System.out.println("Merging SSTables...");

            try {
                sstManager.mergeSST();
            } catch (IOException e) {
                System.err.println("IO error during SST merge: " + e.getMessage());
            }
        }
        }, 0, 5, TimeUnit.MINUTES);
}

}
