package dev.bytekv;

import dev.bytekv.core.KeyValue;
import io.javalin.Javalin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.*;

public class Main {

    private static final Logger log = LoggerFactory.getLogger(Main.class);

    private static final ExecutorService executor = Executors.newFixedThreadPool(
            Runtime.getRuntime().availableProcessors()
    );

    public static void main(String[] args) throws Exception {

        KeyValue kv = new KeyValue("logs", 2500);

        Javalin app = Javalin.create().start(8080);
        log.info("Key value store server running on port 8080 ://");


        app.get("/kv/{key}", ctx -> {
            String key = ctx.pathParam("key");

            kv.get(key) 
              .thenAcceptAsync(value -> {
                  Map<String, Object> response = new HashMap<>();
                  response.put("key", key);
                  response.put("value", value);
                  ctx.json(response);
              }, executor)
              .exceptionally(ex -> {
                  ctx.status(500).json(Map.of("error", ex.getCause().getMessage()));
                  return null;
              });
        });

        app.post("/kv", ctx -> {
            Map<String, Object> json = ctx.bodyAsClass(Map.class);
            String key = (String) json.get("key");
            String value = (String) json.get("value");
            long ttl = json.get("ttl") instanceof Number ? ((Number) json.get("ttl")).longValue() : 0;

            CompletableFuture<String> future;
            if (ttl > 0) {
                future = kv.put(key, value, ttl);
            } else {
                future = kv.put(key, value);
            }

            future.thenAcceptAsync(result -> ctx.json(Map.of("result", result)), executor)
                  .exceptionally(ex -> {
                      ctx.status(500).json(Map.of("error", ex.getCause().getMessage()));
                      return null;
                  });
        });

        app.delete("/kv/{key}", ctx -> {
            String key = ctx.pathParam("key");

            kv.delete(key)
              .thenAcceptAsync(result -> ctx.json(Map.of("result", result)), executor)
              .exceptionally(ex -> {
                  ctx.status(500).json(Map.of("error", ex.getCause().getMessage()));
                  return null;
              });
        });

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                executor.shutdown();
                executor.awaitTermination(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }));
    }
}
