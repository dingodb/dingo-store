/*
 * Copyright 2021, Zetyun DataPortal All rights reserved.
 */

package io.dingodb.sdk.common.concurrent;

import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class ThreadFactoryBuilder {

    public static final String DEFAULT_NAME = "default-thread";


    private String name;
    private boolean daemon = false;
    private int priority = Thread.NORM_PRIORITY;
    private ThreadGroup group = Thread.currentThread().getThreadGroup();

    public ThreadFactoryBuilder name(String name) {
        this.name = name;
        return this;
    }

    public ThreadFactoryBuilder daemon(boolean daemon) {
        this.daemon = daemon;
        return this;
    }

    public ThreadFactoryBuilder priority(int priority) {
        this.priority = priority;
        return this;
    }

    public ThreadFactoryBuilder group(ThreadGroup group) {
        this.group = group;
        return this;
    }

    /**
     * build thread factory.
     */
    public ThreadFactory build() {
        return new ThreadFactory() {
            private final AtomicInteger index = new AtomicInteger(0);

            @Override
            public Thread newThread(@NonNull Runnable runnable) {
                String threadName = String.format("%s-thread-%d", name, this.index.incrementAndGet());
                Thread thread = new Thread(group, runnable, threadName);

                thread.setDaemon(daemon);
                thread.setPriority(priority);

                return thread;
            }
        };
    }
}
