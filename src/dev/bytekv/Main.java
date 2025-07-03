package dev.bytekv;

import dev.bytekv.core.KVStore;

public class Main {
   public static void main(String[] args){
      KVStore kvStore = KVStore.create(40, 20, "wal/master.log", "wal");
      kvStore.compactLogging(true);

      try{
         kvStore.put("charan", "yourmom").get();
         kvStore.put("lmao", "123").get();
    
      }catch(Exception e){
         System.out.print(e.getMessage());
      }

      System.out.println("Done :P");
    
   }

}
