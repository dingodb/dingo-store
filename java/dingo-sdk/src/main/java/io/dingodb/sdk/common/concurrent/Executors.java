/*
 * Copyright 2021 DataCanvas
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.dingodb.sdk.common.concurrent;

import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
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

    public static String threadName() {
        return Thread.currentThread().getName();
    }

    public static Executor executor(String name) {
        return command -> execute(name, command);
    }

    public static void execute(String name, Runnable command) {
        GLOBAL_POOL.execute(wrap(name, command));
    }

    public static void execute(String name, Runnable command, boolean ignoreError) {
        GLOBAL_POOL.execute(wrap(name, command, ignoreError));
    }

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

    private static Runnable wrap(String name, Runnable runnable) {
        return () -> run(name, runnable, false);
    }

    private static Runnable wrap(String name, Runnable runnable, boolean ignoreError) {
        return () -> run(name, runnable, ignoreError);
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

    private static void run(String name, Runnable runnable, boolean ignoreError) {
        Thread thread = Thread.currentThread();
        try {
            if (log.isTraceEnabled()) {
                log.trace("Run [{}] start, thread id [{}], set thread name.", name, thread.getId());
            }
            StringBuilder builder = new StringBuilder(name);
            builder.append("-").append(thread.getId());
            thread.setName(builder.toString());
            runnable.run();
        } catch (Throwable e) {
            if (ignoreError) {
                log.error("Execute {} catch error.", name, e);
            } else {
                log.error("Execute {} catch error.", name, e);
                throw e;
            }
        } finally {
            thread.setName(FREE_THREAD_NAME);
            if (log.isTraceEnabled()) {
                log.trace("Run [{}] finish, thread id [{}], reset thread name.", name, thread.getId());
            }
        }
    }
}
