package hashing;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;


interface HashFunction{
    long hash(String key);
}

public class sha1HashFunction implements HashFunction{
    public long hash(String key){
        try{
            MessageDigest md = MessageDigest.getInstance("SHA-1");
            byte[] digest = md.digest(key.getBytes(StandardCharsets.UTF_8));
                return ByteBuffer.wrap(digest).getLong();
        }catch(NoSuchAlgorithmException e){
            throw new RuntimeException(e);
        }

    }
}   

