/*
 * Copyright 2021, Zetyun DataPortal All rights reserved.
 */

package io.dingodb.sdk.common.concurrent;

import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

@Slf4j
public class Executors {

    private static final String FREE_THREAD_NAME = "FREE";

    public static final String GLOBAL_NAME = "GLOBAL";

    private static final ThreadPoolExecutor GLOBAL_POOL = new ThreadPoolBuilder()
            .name(GLOBAL_NAME)
            .coreThreads(0)
            .maximumThreads(Integer.MAX_VALUE)
            .keepAliveSeconds(TimeUnit.MINUTES.toSeconds(1))
            .workQueue(new SynchronousQueue<>())
            .daemon(true)
            .group(new ThreadGroup(GLOBAL_NAME))
            .build();

    public static <T> CompletableFuture<T> submit(String name, Callable<T> task) {
        CompletableFuture<T> future = new CompletableFuture<>();
        GLOBAL_POOL.execute(() -> {
            try {
                future.complete(wrap(name, task).call());
            } catch (Exception e) {
                future.completeExceptionally(e);
            }
        });
        return future;
    }

    private static <V> Callable<V> wrap(String name, Callable<V> callable) {
        return () -> call(name, callable, false);
    }

    private static <V> V call(String name, Callable<V> callable, boolean ignoreFalse) throws Exception {
        Thread thread = Thread.currentThread();
        try {
            if (log.isTraceEnabled()) {
                log.trace("Call [{}] start, thread id [{}], set thread name.", name, thread.getId());
            }
            StringBuilder builder = new StringBuilder(name);
            builder.append("-").append(thread.getId());
            thread.setName(builder.toString());
            return callable.call();
        } catch (Throwable e) {
            if (ignoreFalse) {
                if (log.isDebugEnabled()) {
                    log.error("Execute {} catch error.", name, e);
                }
                return null;
            } else {
                log.error("Execute {} catch error.", name, e);
                throw e;
            }
        } finally {
            thread.setName(FREE_THREAD_NAME);
            if (log.isTraceEnabled()) {
                log.trace("Call [{}] finish, thread id [{}], reset thread name.", name, thread.getId());
            }
        }
    }
}
