package dev.bytekv;

import dev.bytekv.core.KVStore;

public class BenchMark{
    public static void main(String[] args) throws InterruptedException {

        KVStore kv = KVStore.createDB();
        kv.logging(true);

        long startTime = System.currentTimeMillis();

        for(int i = 0 ; i < 100_000 ; ++i){
                kv.put("charan" + i , "jlawrence");
                if(i%1000 == 0)
                    System.out.println("written " + i + " entries ");

        }

        long endTime = System.currentTimeMillis();
        long result = endTime - startTime;


        System.out.println("time taken: " + result + " ms");
        try{ System.out.println(kv.get("charan0").get()); }

        catch(Exception e){
            System.out.println(e.getMessage());
        }


    }
}
