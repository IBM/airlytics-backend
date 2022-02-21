package jvm;

public class MemEater {
    public static void main(String[] args) {
        System.out.printf("%.3fGiB", Runtime.getRuntime().maxMemory() / (1024.0 * 1024.0 * 1024.0));
        System.out.println();
    }
}