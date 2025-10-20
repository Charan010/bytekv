package dev.bytekv.core;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

public class KeyValue{
        
    Coordinator cood;
    String logPath;
    int memTableLimit;

    public KeyValue(String logPath, int memTableLimit) throws IOException{
        try{ cood = new Coordinator(logPath, memTableLimit); }
        catch(IOException e){
            throw new IOException(e);
        }
    } 

    public String put(String key , String value) throws InterruptedException, ExecutionException{
        if(key == null)
            return "ERROR: null key";
         
        cood.put(key, value);
        return "OK";
    }

    public String put(String key, String value, long ms) throws InterruptedException, ExecutionException{
        if(key == null)
            return "ERROR: null key";

        long currentTime = System.currentTimeMillis() + ms;
        cood.put(key, value, currentTime);
        return "OK";

    }

    public String get(String key) throws InterruptedException, ExecutionException{

        if(key == null)
            return "ERROR: null key";

        return cood.get(key).get();
    }

    public String getTTL(String key)throws InterruptedException, ExecutionException{
        if(key == null)
            return "ERROR: null key";

        return cood.getTTL(key).get();
    }

    public String delete(String key)throws InterruptedException , ExecutionException{
        if(key == null)
            return "ERROR: null key";

        cood.delete(key);
        return "OK";
    }

    public void forceFlush(){
        cood.forceFlush();
    }

    public void shutDown() throws InterruptedException{
        try{
            cood.shutDown();

        }catch(InterruptedException e){
            throw new InterruptedException(e.getMessage());
        }
    }
}