/*package dev.bytekv.core.pubsub;

import java.util.concurrent.BlockingQueue;

public class Publisher {

    private static int MAX_PUBLISHES = 40;

    private String topicName;
    private BlockingQueue<String> queue;

    public Publisher(String topicName){
        this.topicName = topicName;
        this.queue = new BlockingQueue<>(MAX_PUBLISHES);    
    }

    public boolean publishMessage(String message){
        if(this.queue.size() == MAX_PUBLISHES)
            return false;

        this.queue.offer(message);
    }
}

*/