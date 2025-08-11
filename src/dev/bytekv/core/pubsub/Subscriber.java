/*package dev.bytekv.core.pubsub;

import java.io.PrintWriter;
import java.net.Socket;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class Subscriber implements Runnable{
    private final Socket socket;
    private final PrintWriter out;
    private final BlockingQueue<String> messageQueue;
    private volatile boolean active;

    public Subscriber(Socket socket) throws Exception{
        this.socket = socket;
        this.out = new PrintWriter(socket.getOutputStream(), true);
        this.messageQueue = new LinkedTransferQueue<>();
        this.active = true;
    }

    public void sendMessage(String message){
        if(active)
            messageQueue.offer(message);
    }

    public void shutdown(){
        this.active = false;
        try{
            socket.close();
        
        }catch(Exception e){
            // ignore this stupid ahhh 
        }

    }

    @Override
    public void run(){
        try{
        while(active && socket.isAlive()){
            string msg = messageQueue.take();
            out.println(msg);
        }

    }catch(InterruptedException e){
        Thread.currentThread().interrupt();
    
    }catch(Exception e){
        System.out.println("Subscriber error:" + e.getMessage());
    
    }finally{
        shutdown();
    }
    }
}

*/
