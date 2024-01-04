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

package io.dingodb.sdk.service.lock;

import lombok.AllArgsConstructor;
import lombok.experimental.Delegate;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.function.Consumer;

@Slf4j
@Deprecated
public class LockService {

    public class Lock implements java.util.concurrent.locks.Lock {
        private final io.dingodb.sdk.service.LockService.Lock lock;

        public Lock() {
            this.lock = lockService.newLock();
        }

        private Lock(String value) {
            this.lock = lockService.newLock(value);
        }

        private Lock(Consumer<Lock> consumer) {
            this.lock = lockService.newLock($ -> consumer.accept(this));
        }

        public CompletableFuture<Void> watchDestroy() {
            return this.lock.watchDestroy();
        }

        public void lock() {
            this.lock.lock();
        }

        public void lockInterruptibly() throws InterruptedException {
            this.lock.lockInterruptibly();
        }

        public boolean tryLock() {
            return this.lock.tryLock();
        }

        public boolean tryLock(long time, @NonNull TimeUnit unit) throws InterruptedException {
            return this.lock.tryLock(time, unit);
        }

        public void unlock() {
            this.lock.unlock();
        }

        public Condition newCondition() {
            return this.lock.newCondition();
        }

        public int getLocked() {
            return this.lock.getLocked();
        }

        public long getRevision() {
            return this.lock.getRevision();
        }
    }


    @Delegate
    private final io.dingodb.sdk.service.LockService lockService;

    public LockService(String servers) {
        this(servers, 30);
    }

    public LockService(String resource, String servers) {
        this(resource, servers, 30);
    }

    public LockService(String servers, int leaseTtl) {
        this(UUID.randomUUID().toString(), servers, leaseTtl);
    }

    public LockService(String resource, String servers, int leaseTtl) {
        this.lockService = new io.dingodb.sdk.service.LockService(resource, servers, leaseTtl);
    }

    public Lock newLock() {
        return new Lock();
    }

    public Lock newLock(String value) {
        return new Lock(value);
    }

    @Deprecated
    public Lock newLock(Consumer<Lock> onReset) {
        return new Lock(onReset);
    }

}
