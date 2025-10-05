package dev.bytekv;

public class Config {
    private int threadpool;
    private int blockingQueue; 
    private int memtableFlush;
    private int diskFlushLimit;
    private String logFolder;

    public int getThreadpool(){ 
        return threadpool; 
    }

    public void setThreadpool(int threadpool){
         this.threadpool = threadpool; 
    }

    public int getBlockingQueue(){
         return blockingQueue; 
    }

    public void setBlockingQueue(int blockingQueue){ 
        this.blockingQueue = blockingQueue; 
    }

    public int getMemtableFlush(){ 
        return memtableFlush; 
    }

    public void setMemtableFlush(int memtableFlush) {
         this.memtableFlush = memtableFlush; 
    }

    public int getDiskFlushLimit() {
        return diskFlushLimit; 
    }


    public void setDiskFlushLimit(int diskFlushLimit){
         this.diskFlushLimit = diskFlushLimit; 
    }


    public void setLogFolder(String logFolder){
            this.logFolder = logFolder;
    }
    
    public String getLogFolder(){
        return logFolder;
    }


}
