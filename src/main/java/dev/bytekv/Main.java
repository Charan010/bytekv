package dev.bytekv;

import dev.bytekv.core.KeyValue;
import dev.bytekv.core.storage.SSTManager;
import io.javalin.Javalin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.*;

public class Main {

    private static final Logger log = LoggerFactory.getLogger(Main.class);

    private static final ExecutorService writeExecutor = Executors.newFixedThreadPool(
            Runtime.getRuntime().availableProcessors()
    );

    public static void main(String[] args) throws Exception {

        KeyValue kv = new KeyValue("logs", 2500);

        Javalin app = Javalin.create().start(8080);
        log.info("Key value store server running on port 8080 ://");

        app.get("/kv/{key}", ctx -> {
            String key = ctx.pathParam("key");
            try {
                String value = kv.get(key);
                Map<String, Object> response = new HashMap<>();
                response.put("key", key);
                response.put("value", value);
                ctx.json(response);
            } catch (Exception e) {
                ctx.status(500).json(Map.of("error", e.getMessage()));
            }
        });

        app.post("/kv", ctx -> {
            Map<String, Object> json = ctx.bodyAsClass(Map.class);
            String key = (String) json.get("key");
            String value = (String) json.get("value");
            long ttl = json.get("ttl") instanceof Number ? ((Number) json.get("ttl")).longValue() : 0;

            writeExecutor.submit(() -> {
                try {
                    if (ttl > 0) {
                        kv.put(key, value, ttl);
                    } else {
                        kv.put(key, value);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });

            ctx.json(Map.of("result", "OK"));
        });

        app.delete("/kv/{key}", ctx -> {
            String key = ctx.pathParam("key");

            writeExecutor.submit(() -> {
                try {
                    kv.delete(key);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });

            ctx.json(Map.of("result", "OK"));
        });

        app.post("/flush", ctx -> {
            writeExecutor.submit(() -> {
                try {
                    kv.forceFlush();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });

            ctx.json(Map.of("result", "flush started"));
        });

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                writeExecutor.shutdown();
                writeExecutor.awaitTermination(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }));
    }
}
