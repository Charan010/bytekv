package dev.bytekv.core;

//wrapping data into this class for TTL key&value.
public class StoreEntry{
    public String key;
    public byte[] value;
    public boolean TTL;
    public long expiryTime;


    StoreEntry(String key,byte[] value, boolean TTL, long expiryTime){
        this.key = key;
        this.value = value;
        this.TTL = TTL;
        this.expiryTime = expiryTime;
    }
}
