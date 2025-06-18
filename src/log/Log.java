package src.log;

public class Log{
    public static void info(String msg) {
        System.out.println("[" + java.time.LocalTime.now() + "] [INFO] " + msg);
    }

    public static void warn(String msg) {
        System.out.println("[" + java.time.LocalTime.now() + "] [WARN] " + msg);
    }

    public static void error(String msg) {
        System.out.println("[" + java.time.LocalTime.now() + "] [ERROR] " + msg);
    }
}
