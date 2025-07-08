package dev.bytekv;

import dev.bytekv.core.KVStore;

import java.util.Scanner;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

public class Main {
    public static void main(String[] args) {

        KVStore kvStore = KVStore.create(100, 100, "logs/master.log", "logs");
        Scanner scanner = new Scanner(System.in);
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);


        System.out.println("🟢 ByteKV Booted Up!");
        System.out.println("Welcome to ByteKV! Type --help for more commands");

        while (true) {
            try {
                System.out.print("> ");
                String command = scanner.nextLine().trim();
                String[] tokens = command.split(" ");

                if (command.equalsIgnoreCase("exit")) break;

                if (command.equalsIgnoreCase("--help")) {
                    System.out.println();
                    System.out.println("Available commands:");
                    System.out.println("  PUT <key> <value> [ttl]       - Store a value with optional TTL (ms)");
                    System.out.println("  GET <key>                     - Retrieve a value");
                    System.out.println("  DELETE <key> [ttl]            - Delete a value immediately or after TTL");
                    System.out.println("  SETFLAGS --logging=true       - Enable/Disable logging");
                    System.out.println("           --compactlogging=... - Enable/Disable compaction");
                    System.out.println("  STATUS                        - Show current flags");
                    System.out.println("  exit                          - Exit ByteKV");
                    continue;
                }

                if (tokens.length == 0) continue;

                String op = tokens[0].toUpperCase();

                switch (op) {
                    case "GET":
                        if (tokens.length == 2) {
                            String value = kvStore.get(tokens[1]).get();
                            System.out.println(value != null ? value : "NOT_FOUND");
                        } else {
                            System.out.println("Usage: GET <key>");
                        }
                        break;

                    case "PUT":
                        if (tokens.length == 3) {
                            System.out.println(kvStore.put(tokens[1], tokens[2]).get());
                        } else if (tokens.length == 4) {
                            long ttl = Long.parseLong(tokens[3]);
                            System.out.println(kvStore.put(tokens[1], tokens[2], ttl).get());
                        } else {
                            System.out.println("Usage: PUT <key> <value> [ttl]");
                        }
                        break;

                    case "DELETE":
                        if (tokens.length == 2) {
                            String result = kvStore.delete(tokens[1]).get();
                            if(result.equals("__<deleted>__"))
                                result = "NOT KEY FOUND";
                            
                            System.out.println(result != null ? result : "NO KEY FOUND");
                        } else if (tokens.length == 3) {
                            long ttl = Long.parseLong(tokens[2]);
                            String result = kvStore.delete(tokens[1], ttl).get();
                            System.out.println(result != null ? result : "NO KEY FOUND");
                        } else {
                            System.out.println("Usage: DELETE <key> [ttl]");
                        }
                        break;

                    case "SETFLAGS":
                        for (int i = 1; i < tokens.length; i++) {
                            String[] flagParts = tokens[i].split("=");
                            if (flagParts.length != 2) continue;

                            String flag = flagParts[0].toLowerCase();
                            String value = flagParts[1].toLowerCase();

                            if (!value.equals("true") && !value.equals("false")) continue;

                            boolean isEnabled = Boolean.parseBoolean(value);
                            switch (flag) {
                                case "--logging":
                                    kvStore.logging(isEnabled);
                                    System.out.println("Logging: " + value);
                                    break;
                                case "--compactlogging":
                                    kvStore.compactLogging(isEnabled);
                                    System.out.println("Compaction Logging: " + value);
                                    break;
                                default:
                                    System.out.println("Unknown flag: " + flag);
                            }
                        }
                        break;
                    default:
                        System.out.println("Unknown command. Type --help for available commands.");
                }

            } catch (Exception e) {
                System.out.println("ERROR: " + e.getMessage());
            }
        }

        scheduler.shutdown();
        scanner.close();
    }
}
