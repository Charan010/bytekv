package src.hashing;

import java.util.SortedMap;
import java.util.TreeMap;


public class ConsistentHashing{
    private final TreeMap<Long,Node> ring = new TreeMap<>();
    private final HashFunction hashFunction;
    private final int virtualNodes;

     public ConsistentHashing(HashFunction hashFunction, int virtualNodes) {
        this.hashFunction = hashFunction;
        this.virtualNodes = virtualNodes;
    }

    public void addNode(Node node){
        for(int i = 0 ; i < virtualNodes ; ++i){
            long hash = hashFunction.hash(node.getId() + "#" + i);
            ring.put(hash, node);
        }
    }

    public void removeNode(Node node){
        for(int i = 0 ; i < virtualNodes; ++i){
            long hash = hashFunction.hash(node.getId() + "#" + i);
            ring.remove(hash);
        }
    }

     public Node getNode(String key) {
        if (ring.isEmpty()) return null;

        long hash = hashFunction.hash(key);
        SortedMap<Long, Node> tail = ring.tailMap(hash);
        Long nodeHash = tail.isEmpty() ? ring.firstKey() : tail.firstKey();
        return ring.get(nodeHash);
    }
}


