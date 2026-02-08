package dev.bytekv.core.WAL;

public class WalEntry {
    
    String key,value;
    Byte op;

    public WalEntry(String key , String value, Byte op){
        this.key = key;
        this.value = value;
        this.op = op;
    }
}
