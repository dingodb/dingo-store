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
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.nio.charset.StandardCharsets;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Consumer;
import java.util.stream.Collectors;

@Slf4j
public class LockService {

    private final int resourceSepIndex;

    public final String resource;

    public final String resourcePrefixBegin;
    public final String resourcePrefixEnd;

    private final VersionServiceConnector connector;

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
        this(resource, new VersionServiceConnector(servers, leaseTtl));
    }

    private LockService(String resource, VersionServiceConnector connector) {
        this.resource = resource;
        this.connector = connector;
        this.resourcePrefixBegin = (resource + '|' + lease() + "|0|");
        this.resourcePrefixEnd = (resource + '|' + lease() + "|1|");
        this.resourceSepIndex = resource.length() + 1;
    }

    public long lease() {
        int times = 30;
        while (connector.getLease() == -1 && times-- > 0) {
            LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(1));
        }
        if (connector.getLease() == -1) {
            throw new RuntimeException("Can not get lease, please retry.");
        }
        return connector.getLease();
    }

    public List<LockInfo> listLock() {
        return connector.exec(stub -> stub.kvRange(rangeRequest())).getKvsList().stream()
            .map(kv -> new LockInfo(
                kv.getKv().getKey().toStringUtf8(), kv.getKv().getValue().toStringUtf8(), kv.getModRevision()
            )).collect(Collectors.toList());
    }

    public void close() {
        try {
            connector.exec(stub -> stub.kvDeleteRange(deleteAllRangeRequest()));
        } catch (Exception ignore) {
        }
    }

    public Lock newLock() {
        return new Lock("");
    }

    public Lock newLock(String values) {
        return new Lock("");
    }

    @Deprecated
    public Lock newLock(Consumer<Lock> onReset) {
        return new Lock(onReset);
    }

    public class Lock implements java.util.concurrent.locks.Lock {

        public final String lockId = UUID.randomUUID().toString();
        public final String resourceKey = resourcePrefixBegin + lockId;
        public final String resourceValue;

        private final Consumer<Lock> onReset;
        private final CompletableFuture<Void> destroyFuture = new CompletableFuture<>();

        @Getter
        private int locked = 0;
        @Getter
        private long revision;

        @Deprecated
        public Lock(Consumer<Lock> onReset) {
            this.onReset = onReset;
            this.resourceValue = "";
        }

        public Lock(String value) {
            this.onReset = null;
            this.resourceValue = value;
        }

        private synchronized void destroy() {
            if (locked == 0) {
                return;
            }
            if (destroyFuture.isDone()) {
                destroyFuture.complete(null);
            }
            CompletableFuture
                .runAsync(() ->
                    connector.exec(stub -> stub.kvDeleteRange(deleteRangeRequest(resourceKey)))
                ).whenComplete((r, e) -> {
                    if (onReset != null) {
                        onReset.accept(this);
                    }
                    if (e != null) {
                        log.error("Delete {} error when reset.", resourceKey, e);
                        destroy();
                    }
                });
        }

        private boolean locked() {
            if (locked > 0) {
                if (destroyFuture.isDone()) {
                    throw new RuntimeException("The lock destroyed.");
                }
                locked++;
                return true;
            }
            return false;
        }

        public synchronized CompletableFuture<Void> watch() {
            if (locked > 0) {
                return destroyFuture;
            }
            throw new RuntimeException("Cannot watch, not lock.");
        }

        @Override
        public synchronized void lock() {
            if (locked()) {
                return;
            }
            while (true) {
                try {
                    if (connector.getLease() == -1) {
                        LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(1));
                        continue;
                    }
                    Version.PutResponse response = connector.exec(stub -> stub.kvPut(putRequest(resourceKey)));
                    long revision = response.getHeader().getRevision();
                    Version.RangeResponse rangeResponse = connector.exec(stub -> stub.kvRange(rangeRequest()));
                    if (rangeResponse.getKvsList().isEmpty()) {
                        throw new RuntimeException("Put " + resourceKey + " success, but range is empty.");
                    }
                    Version.Kv current = rangeResponse.getKvsList().stream()
                        .min(Comparator.comparingLong(Version.Kv::getModRevision))
                        .get();
                    if (current.getModRevision() == revision) {
                        this.revision = revision;
                        if (log.isDebugEnabled()) {
                            log.debug(
                                "Lock {} success use {} revision, current locks: {}.",
                                resourceKey, revision, rangeResponse.getKvsList()
                            );
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
                    try {
                        connector.exec(
                            stub -> stub.watch(watchRequest(previous.getKv().getKey(), previous.getModRevision())),
                            __ -> ErrorCodeUtils.Strategy.IGNORE
                        );                    } catch (Exception ignored) {
                    }
                } catch (Exception e) {
                    log.error("Lock {} error, id: {}", resourceKey, lockId, e);
                }
            }
        }

        @Override
        public synchronized void lockInterruptibly() throws InterruptedException {
            throw new UnsupportedOperationException();
        }

        @Override
        public synchronized boolean tryLock() {
            if (locked()) {
                return true;
            }
            if (connector.getLease() == -1) {
                return false;
            }
            try {
                Version.PutResponse response = connector.exec(stub -> stub.kvPut(putRequest(resourceKey)));
                long revision = response.getHeader().getRevision();
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
            if (locked()) {
                return true;
            }
            try {
                Version.PutResponse response = connector.exec(stub -> stub.kvPut(putRequest(resourceKey)));
                long revision = response.getHeader().getRevision();
                while (time-- > 0) {
                    if (connector.getLease() == -1) {
                        LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(1));
                    }
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
                log.error("Try lock error.", e);
            }

            connector.exec(stub -> stub.kvDeleteRange(deleteRangeRequest(resourceKey)));
            return false;
        }


        private void watchLock(Version.Kv kv) {
            CompletableFuture.supplyAsync(() ->
                connector.exec(stub -> stub.watch(watchRequest(kv.getKv().getKey(), kv.getModRevision())),
                connector.leaseTtl,
                __ -> ErrorCodeUtils.Strategy.RETRY
            )).whenComplete((r, e) -> {
                if (e != null) {
                    log.error("Watch locked error, or watch retry time great than lease ttl.", e);
                }
                if (r.getEventsList().stream().anyMatch(event -> event.getType() == Version.Event.EventType.DELETE)) {
                    this.destroy();
                }
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
                        .setKey(ByteString.copyFromUtf8(resourceKey))
                        .build())
                .setNeedPrevKv(true)
                .build();
    }

    private Version.RangeRequest rangeRequest() {
        return Version.RangeRequest.newBuilder()
            .setKey(ByteString.copyFromUtf8(resourcePrefixBegin))
            .setRangeEnd(ByteString.copyFromUtf8(resourcePrefixEnd))
            .build();
    }

    private Version.DeleteRangeRequest deleteRangeRequest(String resourceKey) {
        return Version.DeleteRangeRequest.newBuilder()
            .setKey(ByteString.copyFrom(resourceKey.getBytes(StandardCharsets.UTF_8)))
            .build();
    }

    private Version.DeleteRangeRequest deleteAllRangeRequest() {
        return Version.DeleteRangeRequest.newBuilder()
            .setKey(ByteString.copyFromUtf8(resourcePrefixBegin))
            .setRangeEnd(ByteString.copyFromUtf8(resourcePrefixEnd))
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
