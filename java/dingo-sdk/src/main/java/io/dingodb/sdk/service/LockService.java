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

package io.dingodb.sdk.service;

import io.dingodb.sdk.common.DingoClientException;
import io.dingodb.sdk.common.utils.Future;
import io.dingodb.sdk.service.entity.common.KeyValue;
import io.dingodb.sdk.service.entity.meta.TsoOpType;
import io.dingodb.sdk.service.entity.meta.TsoRequest;
import io.dingodb.sdk.service.entity.meta.TsoTimestamp;
import io.dingodb.sdk.service.entity.version.DeleteRangeRequest;
import io.dingodb.sdk.service.entity.version.Event;
import io.dingodb.sdk.service.entity.version.EventFilterType;
import io.dingodb.sdk.service.entity.version.Kv;
import io.dingodb.sdk.service.entity.version.LeaseGrantRequest;
import io.dingodb.sdk.service.entity.version.LeaseRenewRequest;
import io.dingodb.sdk.service.entity.version.PutRequest;
import io.dingodb.sdk.service.entity.version.PutResponse;
import io.dingodb.sdk.service.entity.version.RangeRequest;
import io.dingodb.sdk.service.entity.version.RangeResponse;
import io.dingodb.sdk.service.entity.version.WatchRequest;
import io.dingodb.sdk.service.entity.version.WatchRequest.RequestUnionNest.OneTimeRequest;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Consumer;

import static io.dingodb.sdk.service.entity.version.EventType.DELETE;
import static io.dingodb.sdk.service.entity.version.EventType.NOT_EXISTS;
import static java.nio.charset.StandardCharsets.UTF_8;

@Slf4j
public class LockService {

    private final ScheduledExecutorService executors = Executors.newScheduledThreadPool(1);
    private final MetaService tsoService;
    private ScheduledFuture<?> renewFuture;

    public final long leaseTtl;
    private volatile long lease = -1;
    private final int delay;
    private final VersionService kvService;
    private final int resourceSepIndex;

    public final String resource;

    public final String resourcePrefixBegin;
    public final String resourcePrefixEnd;

    private String resourcePrefixKeyBegin;
    private String resourcePrefixKeyEnd;

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
        this.kvService = Services.versionService(Services.parse(servers));
        this.tsoService = Services.tsoService(Services.parse(servers));
        this.resource = resource;
        this.resourceSepIndex = resource.length() + 1;
        this.leaseTtl = leaseTtl;
        this.resourcePrefixBegin = resource + "|0|";
        this.resourcePrefixEnd = resource + "|1|";
        this.delay = Math.max(Math.abs(leaseTtl * 1000) / 3, 1000);
        this.executors.execute(this::grantLease);
    }

    private synchronized void grantLease() {
        do {
            try {
                long ts = lease;
                if (ts == -1) {
                    TsoTimestamp tso = tsoService.tsoService(
                        TsoRequest.builder().count(1).opType(TsoOpType.OP_GEN_TSO).build()).getStartTimestamp();
                    ts = (tso.getPhysical() << 18) + tso.getLogical();
                }
                lease = kvService.leaseGrant(LeaseGrantRequest.builder().iD(ts).tTL(leaseTtl).build()).getID();
            } catch (Exception e) {
                if (lease == -1) {
                    log.error("Grant lease failed, will retry...", e);
                    LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(1));
                } else {
                    log.error("Grant lease again failed.", e);
                }
            }
        } while (lease == -1);
        resourcePrefixKeyBegin = (resourcePrefixBegin + lease() + "|0|");
        resourcePrefixKeyEnd = (resourcePrefixBegin + lease() + "|1|");
        if (renewFuture == null) {
            renewFuture = executors.scheduleWithFixedDelay(this::renewLease, delay, delay, TimeUnit.MILLISECONDS);
        }
    }

    private void renewLease() {
        if (lease == -1) {
            return;
        }
        try {
            kvService.leaseRenew(LeaseRenewRequest.builder().iD(lease()).build());
        } catch (Exception e) {
            log.error("Renew lease {} error, grant again.", lease, e);
            grantLease();
        }
    }

    public long lease() {
        while (lease == -1) {
            LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(1));
        }
        return lease;
    }

    public String getResourcePrefixKeyBegin() {
        while (resourcePrefixKeyBegin == null) {
            LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(1));
        }
        return resourcePrefixKeyBegin;
    }

    public String getResourcePrefixKeyEnd() {
        while (resourcePrefixKeyEnd == null) {
            LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(1));
        }
        return resourcePrefixKeyEnd;
    }

    public List<Kv> listLock() {
        return kvService.kvRange(rangeRequest()).getKvs();
    }

    public Kv currentLock() {
        return listLock().stream()
            .filter(Objects::nonNull)
            .min(Comparator.comparingLong(Kv::getCreateRevision))
            .orElse(null);
    }

    public void close() {
        try {
            kvService.kvDeleteRange(deleteAllRangeRequest());
        } catch (Exception ignore) {
        }
    }

    public Kv put(long ts, String key, String value) {
        PutRequest request = putRequest(key, value);
        PutResponse putResponse = kvService.kvPut(ts, request);
        long createRevision = putResponse.getHeader().getRevision();
        long modRevision = putResponse.getHeader().getRevision();
        if (putResponse.getPrevKv() != null) {
            createRevision = putResponse.getPrevKv().getCreateRevision();
        }
        return Kv.builder()
            .kv(request.getKeyValue())
            .createRevision(createRevision)
            .modRevision(modRevision)
            .build();
    }

    public void delete(long ts, String key) {
        kvService.kvDeleteRange(
            ts, deleteRangeRequest(key)
        );
    }

    public Lock newLock() {
        log.debug("Create new lock with empty value, lease [{}].", lease());
        return new Lock("");
    }

    public Lock newLock(String value) {
        log.debug("Create new lock with [{}], lease [{}].", value, lease());
        return new Lock(value);
    }

    @Deprecated
    public Lock newLock(Consumer<Lock> onReset) {
        log.debug("Create new lock with empty value, lease [{}].", lease());
        return new Lock(onReset);
    }

    public class Lock implements java.util.concurrent.locks.Lock {

        public final String lockId = UUID.randomUUID().toString();
        public final String resourceKey = getResourcePrefixKeyBegin() + lockId;
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
                    kvService.kvDeleteRange(deleteRangeRequest(resourceKey))
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
                    return false;
                }
                locked++;
                return true;
            }
            return false;
        }

        public Future watchDestroy() {
            return new Future(destroyFuture);
        }

        private boolean isLockRevision(long revision, RangeResponse rangeResponse) {
            if (rangeResponse.getKvs().isEmpty()) {
                throw new RuntimeException("Put " + resourceKey + " success, but range is empty.");
            }
            Kv current = rangeResponse.getKvs().stream().filter(Objects::nonNull)
                .min(Comparator.comparingLong(Kv::getCreateRevision))
                .get();
            if (current.getCreateRevision() == revision) {
                this.revision = revision;
                if (log.isDebugEnabled()) {
                    log.debug(
                        "Lock {} success use {} revision, current locks: {}.",
                        resourceKey, revision, rangeResponse.getKvs()
                    );
                }
                if (!destroyFuture.isDone()) {
                    locked++;
                    watchLock(current, this::destroy);
                }
                return true;
            }
            return false;
        }

        private long getCreateRevision(PutResponse response) {
            if (response.getPrevKv() == null) {
                return response.getHeader().getRevision();
            } else {
                return response.getPrevKv().getCreateRevision();
            }
        }

        @Override
        public synchronized void lock() {
            if (locked()) {
                return;
            }
            while (true) {
                try {
                    PutResponse response = kvService.kvPut(putRequest(resourceKey, resourceValue));
                    long revision = getCreateRevision(response);
                    RangeResponse rangeResponse = kvService.kvRange(rangeRequest());
                    if (isLockRevision(revision, rangeResponse)) {
                        break;
                    }
                    Kv previous = rangeResponse.getKvs().stream().filter(Objects::nonNull)
                        .filter(__ -> __.getCreateRevision() < revision)
                        .max(Comparator.comparingLong(Kv::getCreateRevision))
                        .orElseThrow(() -> new RuntimeException("Put " + resourceKey + " success, but no previous."));
                    if (log.isDebugEnabled()) {
                        log.debug("Lock {} wait...", resourceKey);
                    }
                    try {
                        kvService.watch(watchRequest(previous.getKv().getKey(), previous.getCreateRevision()));
                        if (isLockRevision(revision, kvService.kvRange(rangeRequest()))) {
                            break;
                        }
                    } catch (Exception ignored) {
                    }
                } catch (Exception e) {
                    log.error("Lock {} error, id: {}", resourceKey, lockId, e);
                }
            }
            if (destroyFuture.isDone()) {
                throw new RuntimeException("Destroyed!");
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
            if (lease == -1) {
                return false;
            }
            try {
                PutResponse response = kvService.kvPut(putRequest(resourceKey, resourceValue));
                long revision = getCreateRevision(response);
                Optional<Kv> current = kvService.kvRange(rangeRequest())
                    .getKvs().stream()
                    .min(Comparator.comparingLong(Kv::getCreateRevision));
                if (current.map(Kv::getCreateRevision).filter(__ -> __ == revision).isPresent()) {
                    locked++;
                    watchLock(current.get(), this::destroy);
                    return true;
                }
            } catch (Exception e) {
                log.error("Try lock error.", e);
            }

            kvService.kvDeleteRange(deleteRangeRequest(resourceKey));
            return false;
        }

        @Override
        public synchronized boolean tryLock(long time, @NonNull TimeUnit unit) throws InterruptedException {
            if (locked()) {
                return true;
            }
            try {
                PutResponse response = kvService.kvPut(putRequest(resourceKey, resourceValue));
                long revision = getCreateRevision(response);
                while (time-- > 0) {
                    RangeResponse rangeResponse = kvService.kvRange(rangeRequest());
                    Kv current = rangeResponse.getKvs().stream().filter(Objects::nonNull)
                        .min(Comparator.comparingLong(Kv::getCreateRevision))
                        .orElseThrow(() -> new RuntimeException("Put " + resourceKey + " success, but range is empty."));
                    if (current.getCreateRevision() == revision) {
                        if (log.isDebugEnabled()) {
                            log.debug("Lock {} wait...", resourceKey);
                        }
                        if (!destroyFuture.isDone()) {
                            locked++;
                            watchLock(current, this::destroy);
                            return true;
                        }
                        throw new RuntimeException("Destroyed!");
                    }
                    LockSupport.parkNanos(unit.toNanos(1));
                    if (Thread.interrupted()) {
                        throw new InterruptedException();
                    }
                }
            } catch (InterruptedException interruptedException) {
                kvService.kvDeleteRange(deleteRangeRequest(resourceKey));
                throw interruptedException;
            } catch (Exception e) {
                log.error("Try lock error.", e);
            }

            kvService.kvDeleteRange(deleteRangeRequest(resourceKey));
            return false;
        }

        @Override
        public synchronized void unlock() {
            if (locked == 0) {
                return;
            }
            if (--locked == 0) {
                kvService.kvDeleteRange(deleteRangeRequest(resourceKey));
            }
        }

        @Override
        public synchronized Condition newCondition() {
            throw new UnsupportedOperationException();
        }
    }

    public void watchLock(Kv kv, Runnable task) {
        CompletableFuture.supplyAsync(() ->
            kvService.watch(watchRequest(kv.getKv().getKey(), kv.getModRevision()))
        ).whenCompleteAsync((r, e) -> {
            if (e != null) {
                if (!(e instanceof DingoClientException)) {
                    watchLock(kv, task);
                    return;
                }
                log.error("Watch locked error, or watch retry time great than lease ttl.", e);
                return;
            }
            if (r.getEvents().stream().map(Event::getType).anyMatch(type -> type == DELETE || type == NOT_EXISTS)) {
                task.run();
            } else {
                watchLock(kv, task);
            }
        });
    }

    private PutRequest putRequest(String resourceKey, String value) {
        return PutRequest.builder()
            .lease(lease())
            .ignoreValue(value == null || value.isEmpty())
            .keyValue(KeyValue.builder()
                .key(resourceKey.getBytes(UTF_8))
                .value(value == null ? null : value.getBytes(UTF_8))
                .build())
            .needPrevKv(true)
            .build();
    }

    private RangeRequest rangeRequest() {
        return RangeRequest.builder()
            .key(resourcePrefixBegin.getBytes(UTF_8))
            .rangeEnd(resourcePrefixEnd.getBytes(UTF_8))
            .build();
    }

    private DeleteRangeRequest deleteRangeRequest(String resourceKey) {
        return DeleteRangeRequest.builder()
            .key(resourceKey.getBytes(UTF_8))
            .build();
    }

    private DeleteRangeRequest deleteAllRangeRequest() {
        return DeleteRangeRequest.builder()
            .key(resourcePrefixKeyBegin.getBytes(UTF_8))
            .rangeEnd(resourcePrefixKeyEnd.getBytes(UTF_8))
            .build();
    }

    private WatchRequest watchRequest(String resourceKey, long revision) {
        return watchRequest(resourceKey.getBytes(UTF_8), revision);
    }

    private WatchRequest watchRequest(byte[] resourceKey, long revision) {
        return WatchRequest.builder()
            .requestUnion(OneTimeRequest.builder()
                .key(resourceKey)
                .needPrevKv(true)
                .startRevision(revision)
                .filters(Collections.singletonList(EventFilterType.NOPUT))
                .build()
            ).build();
    }

}
