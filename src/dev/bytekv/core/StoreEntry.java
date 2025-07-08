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

    @Override
    public boolean equals(Object o) {
    if (this == o) return true;
        if (!(o instanceof StoreEntry)) return false;
        StoreEntry that = (StoreEntry) o;
        return key.equals(that.key);
    }

    @Override
    public int hashCode() {
        return key.hashCode();
    }

}
