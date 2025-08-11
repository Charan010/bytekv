package dev.bytekv;

import dev.bytekv.core.KVStore;

import java.io.FileInputStream;
import java.util.Scanner;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.Map;

import org.yaml.snakeyaml.Yaml;

public class Main {
    public static void main(String[] args) {

        Yaml yaml = new Yaml();

        try (FileInputStream fis = new FileInputStream("config.yaml")) {
            Map<String, Object> data = yaml.load(fis);

            Map<String, Object> server = (Map<String, Object>) data.get("server");
            int port = (Integer) server.get("port");
            String host = (String) server.get("host");

            System.out.println("Host: " + host);
            System.out.println("Port: " + port);
        
        }catch(Exception e){
            System.out.println(e.getMessage());
        }
    }
}