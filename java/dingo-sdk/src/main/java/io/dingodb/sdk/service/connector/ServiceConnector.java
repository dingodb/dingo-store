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

package io.dingodb.sdk.service.connector;

import io.dingodb.error.ErrorOuterClass;
import io.dingodb.sdk.common.DingoClientException;
import io.dingodb.sdk.common.Location;
import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import io.grpc.ManagedChannel;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.AbstractBlockingStub;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Function;
import java.util.function.Predicate;

@Slf4j
public abstract class ServiceConnector<S extends AbstractBlockingStub<S>> {

    @Getter
    @AllArgsConstructor
    public static class Response<R> {
        private final ErrorOuterClass.Error error;
        private final R response;
    }

    protected final AtomicReference<ManagedChannel> channelRef = new AtomicReference<>();
    protected Set<Location> locations = new CopyOnWriteArraySet<>();

    protected S stub;

    public ServiceConnector(Set<Location> locations) {
        this.locations.addAll(locations);
    }

    public S getStub() {
        return stub;
    }

    public <R> Response<R> exec(Function<S, Response<R>> function) {
        return exec(function, 30, e -> true);
    }

    public <R> Response<R> exec(Function<S, Response<R>> function, Predicate<ErrorOuterClass.Error> retryCheck) {
        return exec(function, 30, retryCheck);
    }

    public <R> Response<R> exec(Function<S, Response<R>> function, int retryTimes, Predicate<ErrorOuterClass.Error> retryCheck) {
        S stub;
        while (retryTimes-- > 0) {
            if ((stub = getStub()) == null) {
                LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(1));
                refresh();
                continue;
            }
            try {
                Response<R> response = function.apply(stub);
                if (response.getError().getErrcodeValue() != 0) {
                    if (retryCheck.test(response.error)) {
                        LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(1));
                        continue;
                    }
                    throw new DingoClientException(response.getError().getErrcodeValue(), response.getError().getErrmsg());
                }
                return response;
            } catch (StatusRuntimeException ignore) {
                LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(1));
            }
        }
        throw new RuntimeException("Retry attempts exhausted, failed to exec operation.");
    }

    public synchronized boolean refresh() {
        stub = null;
        ManagedChannel channel  = channelRef.get();
        channelRef.compareAndSet(channel, null);
        if (channel != null && !channel.isShutdown()) {
            channel.shutdown();
        }
        for (Location location : locations) {
            try {
                channel = transformToLeaderChannel(newChannel(location.getHost(), location.getPort()));
                if (channel != null) {
                    stub = newStub(channel);
                    channelRef.compareAndSet(null, channel);
                }
            } catch (Exception e) {
                log.warn("Connect {} and transform to leader error.", location, e);
            }
        }
        return stub != null;
    }

    protected ManagedChannel newChannel(String host, int port) {
        return Grpc.newChannelBuilder(host + ":" + port, InsecureChannelCredentials.create()).build();
    }

    protected abstract ManagedChannel transformToLeaderChannel(ManagedChannel channel);

    protected abstract S newStub(ManagedChannel channel);

    public Set<Location> getLocations() {
        return locations;
    }

    public void shutdown() {
        ManagedChannel channel = channelRef.getAndSet(null);
        if (channel != null && !channel.isShutdown()) {
            channel.shutdown();
        }
    }
}
