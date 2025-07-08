package dev.bytekv.ttl;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;


import dev.bytekv.core.*;

/*
    TTL manager responsibilities
    run for every x time period
    take random keys and check for expire . if atleast 25% were expired from that random sample. 
    do complete scan otherwise leave it out.
 */

public class TTLManager implements Runnable{
    private KeyValue keyValue;
    List<StoreEntry> values;

    public TTLManager(KeyValue keyValue){
        this.keyValue = keyValue;
    }

    @Override
    public void run(){
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

        scheduler.scheduleAtFixedRate(() ->{

        int expiredCount = 0;

        /* converting hashsets into a list to "get" more randomly */
        this.values = new ArrayList<>(keyValue.ttlEntries);
        Random rand = ThreadLocalRandom.current();

        for(int i = 0; i < Math.min(20, values.size()); ++i){
            StoreEntry se = values.get(rand.nextInt(values.size()));
            
            if(se.TTL && System.currentTimeMillis() > se.expiryTime)
                ++expiredCount;
            
        }
        if(expiredCount >= Math.max(5, values.size() / 4)){
            System.out.println("----- Found 5+ expired TTL entries. running full scan -----");
            ttlChecker();
        }

        },0 ,10, TimeUnit.SECONDS);
    }

    private void ttlChecker() {

    Iterator<StoreEntry> it = keyValue.ttlEntries.iterator();

    while (it.hasNext()) {
        StoreEntry se = it.next();
        if (se.TTL && System.currentTimeMillis() >= se.expiryTime) 
            it.remove();
    }
    }
}