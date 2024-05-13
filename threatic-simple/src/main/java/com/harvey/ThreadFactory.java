package com.harvey;

/**
 * @author harvey
 */
public class ThreadFactory {
    public static Thread newThread(Runnable task) {
        Thread thread = new Thread(task);
        thread.setDaemon(false);
        thread.setPriority(Thread.NORM_PRIORITY);
        return thread;
    }
}
