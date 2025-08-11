package dev.bytekv.core.storage;


/*
    A probabilistic shit which can give you false positives but never false negatives.

    im passing key into 3 hashes and storing the bits.once key is added, it will deffo show in bitset
    so no chance of falsly telling that it doesnt exist.

    keys can collide with other keys. so it can give false positive.
     .its worth when we need to read off huge files.


 */


import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.BitSet;

public class BloomFilter {
    private final BitSet bitset;
    private final int size;

    public BloomFilter(int size) {
        this.size = size;
        this.bitset = new BitSet(size);
    }

    public void add(String key) {
        int[] hashes = getHashes(key);
        for (int hash : hashes) {
            bitset.set(Math.abs(hash % size), true);
        }
    }

    public boolean mightContain(String key) {
        int[] hashes = getHashes(key);
        for (int hash : hashes) {
            if (!bitset.get(Math.abs(hash % size))) {
                return false;
            }
        }
        return true;
    }

    private int[] getHashes(String key) {
        return new int[]{
            sha256Hash(key),
            murmurHash3(key),
            fnv1aHash(key)
        };
    }

    private int sha256Hash(String key) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] hash = md.digest(key.getBytes(StandardCharsets.UTF_8));
            return byteArrayToInt(hash);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("SHA-256 not available", e);
        }
    }

    private int murmurHash3(String key) {
        byte[] data = key.getBytes(StandardCharsets.UTF_8);
        int hash = 0x9747b28c; // seed
        for (byte b : data) {
            hash ^= b;
            hash *= 0x5bd1e995;
            hash ^= hash >>> 15;
        }
        return hash;
    }

    private int fnv1aHash(String key) {
        final int FNV_PRIME = 0x01000193;
        int hash = 0x811c9dc5;
        for (byte b : key.getBytes(StandardCharsets.UTF_8)) {
            hash ^= b;
            hash *= FNV_PRIME;
        }
        return hash;
    }

    private int byteArrayToInt(byte[] b) {
        int result = 0;
        for (int i = 0; i < 4 && i < b.length; i++) {
            result = (result << 8) | (b[i] & 0xff);
        }
        return result;
    }
}