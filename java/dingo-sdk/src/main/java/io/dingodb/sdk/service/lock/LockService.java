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

import com.google.protobuf.ByteString;
import io.dingodb.common.Common;
import io.dingodb.sdk.common.utils.ErrorCodeUtils;
import io.dingodb.sdk.service.connector.VersionServiceConnector;
import io.dingodb.version.Version;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.nio.charset.StandardCharsets;
import java.util.Comparator;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Consumer;

@Slf4j
public class LockService {

    private final int resourceSepIndex;

    public final String resource;

    public final String resourcePrefix;

    public final VersionServiceConnector connector;

    public LockService(String servers) {
        this(servers, 30);
    }

    public LockService(String servers, String resource) {
        this(servers, resource, 30);
    }

    public LockService(String servers, int leaseTtl) {
        this(servers, UUID.randomUUID().toString(), leaseTtl);
    }

    public LockService(String servers, String resource, int leaseTtl) {
        this(new VersionServiceConnector(servers, leaseTtl), resource);
    }

    private LockService(VersionServiceConnector connector, String resource) {
        this.resource = resource;
        this.connector = connector;
        this.resourcePrefix = resource + "|0|";
        this.resourceSepIndex = resource.length() + 1;
    }

    public void close() {
        connector.exec(stub -> stub.kvDeleteRange(deleteAllRangeRequest(resourcePrefix)));
        connector.close();
    }

    public Lock newLock() {
        return new Lock(null);
    }

    public Lock newLock(Consumer<Lock> onReset) {
        return new Lock(onReset);
    }

    public class Lock implements java.util.concurrent.locks.Lock {

        public final String lockId = UUID.randomUUID().toString();
        public final String resourceKey = resource + "|0|" + lockId;

        private final Consumer<Lock> onReset;

        private int locked = 0;

        public Lock(Consumer<Lock> onReset) {
            this.onReset = onReset;
        }

        private synchronized void reset() {
            if (locked == 0) {
                return;
            }
            CompletableFuture
                .runAsync(this::unlock)
                .whenComplete((r, e) -> {
                    if (onReset != null) {
                        onReset.accept(this);
                    }
                    if (e != null) {
                        log.error("Delete {} error when reset.", resourceKey, e);
                    }
                });
        }

        private boolean checkLock() {
            if (locked > 0) {
                locked++;
                return true;
            }
            return false;
        }

        @Override
        public synchronized void lock() {
            if (checkLock()) {
                return;
            }
            try {
                Version.PutResponse response = connector.exec(stub -> stub.kvPut(putRequest(resourceKey)));
                long revision = response.getHeader().getRevision();
                while (true) {
                    Version.RangeResponse rangeResponse = connector.exec(stub -> stub.kvRange(rangeRequest()));
                    if (rangeResponse.getKvsList().isEmpty()) {
                        throw new RuntimeException("Put " + resourceKey + " success, but range is empty.");
                    }
                    Version.Kv current = rangeResponse.getKvsList().stream()
                        .min(Comparator.comparingLong(Version.Kv::getModRevision))
                        .get();
                    if (current.getModRevision() == revision) {
                        if (log.isDebugEnabled()) {
                            log.debug("Lock {} success.", resourceKey);
                        }
                        locked++;
                        watchLock(current);
                        return;
                    }
                    Version.Kv previous = rangeResponse.getKvsList().stream()
                        .filter(__ -> __.getModRevision() < revision)
                        .max(Comparator.comparingLong(Version.Kv::getModRevision))
                        .orElseThrow(() -> new RuntimeException("Put " + resourceKey + " success, but no previous."));
                    if (log.isDebugEnabled()) {
                        log.debug("Lock {} wait...", resourceKey);
                    }
                    connector.exec(
                        stub -> stub.watch(watchRequest(previous.getKv().getKey(), previous.getModRevision())),
                        __ -> ErrorCodeUtils.InternalCode.IGNORE
                    );
                }
            } catch (Exception e) {
                log.error("Lock {} error, id: {}", resourceKey, lockId, e);
            }
            connector.exec(stub -> stub.kvDeleteRange(deleteRangeRequest(resourceKey)));
        }

        @Override
        public synchronized void lockInterruptibly() throws InterruptedException {
            throw new UnsupportedOperationException();
        }

        @Override
        public synchronized boolean tryLock() {
            if (checkLock()) {
                return true;
            }
            Version.PutResponse response = connector.exec(stub -> stub.kvPut(putRequest(resourceKey)));
            long revision = response.getHeader().getRevision();

            try {
                Optional<Version.Kv> current = connector.exec(stub -> stub.kvRange(rangeRequest()))
                    .getKvsList().stream()
                    .min(Comparator.comparingLong(Version.Kv::getModRevision));
                if (current.map(Version.Kv::getModRevision).filter(__ -> __ == revision).isPresent()) {
                    locked++;
                    watchLock(current.get());
                    return true;
                }
            } catch (Exception e) {
                log.error("Try lock error.", e);
            }

            connector.exec(stub -> stub.kvDeleteRange(deleteRangeRequest(resourceKey)));
            return false;
        }

        @Override
        public synchronized boolean tryLock(long time, @NonNull TimeUnit unit) throws InterruptedException {
            if (checkLock()) {
                return true;
            }
            Version.PutResponse response = connector.exec(stub -> stub.kvPut(putRequest(resourceKey)));
            long revision = response.getHeader().getRevision();
            try {
                while (time-- > 0) {
                    Version.RangeResponse rangeResponse = connector.exec(stub -> stub.kvRange(rangeRequest()));
                    Version.Kv current = rangeResponse.getKvsList().stream()
                        .min(Comparator.comparingLong(Version.Kv::getModRevision))
                        .orElseThrow(() -> new RuntimeException("Put " + resourceKey + " success, but range is empty."));
                    if (current.getModRevision() == revision) {
                        if (log.isDebugEnabled()) {
                            log.debug("Lock {} wait...", resourceKey);
                        }
                        locked++;
                        watchLock(current);
                        return true;
                    }
                    LockSupport.parkNanos(unit.toNanos(1));
                    if (Thread.interrupted()) {
                        throw new InterruptedException();
                    }
                }
            } catch (InterruptedException interruptedException) {
                connector.exec(stub -> stub.kvDeleteRange(deleteRangeRequest(resourceKey)));
                throw interruptedException;
            } catch (Exception e) {
                e.printStackTrace();
            }

            connector.exec(stub -> stub.kvDeleteRange(deleteRangeRequest(resourceKey)));
            return false;
        }


        private void watchLock(Version.Kv kv) {
            CompletableFuture.runAsync(() -> {
                connector.exec(
                    stub -> stub.watch(watchRequest(kv.getKv().getKey(), kv.getModRevision())),
                    connector.leaseTtl,
                    __ -> ErrorCodeUtils.InternalCode.RETRY
                );
            }).whenComplete((r, e) -> {
                if (e != null) {
                    log.error("Watch locked error, or watch retry time great than lease ttl.", e);
                }
                this.reset();
            });
        }

        @Override
        public synchronized void unlock() {
            if (locked == 0) {
                return;
            }
            if (--locked == 0) {
                connector.exec(stub -> stub.kvDeleteRange(deleteRangeRequest(resourceKey)));
            }
        }

        @Override
        public synchronized Condition newCondition() {
            throw new UnsupportedOperationException();
        }
    }

    private Version.PutRequest putRequest(String resourceKey) {
        return Version.PutRequest.newBuilder()
                .setLease(connector.getLease())
            .setIgnoreValue(true)
                .setKeyValue(Common.KeyValue.newBuilder()
                        .setKey(ByteString.copyFrom(resourceKey.getBytes(StandardCharsets.UTF_8)))
                        .build())
                .setNeedPrevKv(true)
                .build();
    }

    private Version.RangeRequest rangeRequest() {
        byte[] end = resourcePrefix.getBytes(StandardCharsets.UTF_8);
        end[resourceSepIndex]++;
        return Version.RangeRequest.newBuilder()
            .setKey(ByteString.copyFrom(resourcePrefix.getBytes(StandardCharsets.UTF_8)))
            .setRangeEnd(ByteString.copyFrom(end))
            .build();
    }

    private Version.DeleteRangeRequest deleteRangeRequest(String resourceKey) {
        return Version.DeleteRangeRequest.newBuilder()
            .setKey(ByteString.copyFrom(resourceKey.getBytes(StandardCharsets.UTF_8)))
            .build();
    }

    public Version.DeleteRangeRequest deleteAllRangeRequest(String resourcePrefix) {
        byte[] end = resourcePrefix.getBytes(StandardCharsets.UTF_8);
        end[resourceSepIndex]++;
        return Version.DeleteRangeRequest.newBuilder()
            .setKey(ByteString.copyFrom(resourcePrefix.getBytes(StandardCharsets.UTF_8)))
            .setRangeEnd(ByteString.copyFrom(end))
            .build();
    }

    private Version.WatchRequest watchRequest(ByteString resourceKey, long revision) {
        return Version.WatchRequest.newBuilder().setOneTimeRequest(
                Version.OneTimeWatchRequest.newBuilder()
                        .setKey(resourceKey)
                        .setNeedPrevKv(true)
                        .setStartRevision(revision)
                        .build()
        ).build();
    }

}
