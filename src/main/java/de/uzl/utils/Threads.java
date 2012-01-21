package de.uzl.utils;

public class Threads {
    public static void trySleep(int millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } 
    }

}
