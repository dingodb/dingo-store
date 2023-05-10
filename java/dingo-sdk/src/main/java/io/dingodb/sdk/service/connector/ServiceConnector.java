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
import io.dingodb.sdk.common.utils.Optional;
import io.grpc.ManagedChannel;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.AbstractBlockingStub;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Function;
import java.util.function.Predicate;

import static io.dingodb.sdk.common.utils.ErrorCodeUtils.refreshCode;
import static io.dingodb.sdk.common.utils.NoBreakFunctions.wrap;

@Slf4j
public abstract class ServiceConnector<S extends AbstractBlockingStub<S>> {

    private final AtomicBoolean refresh = new AtomicBoolean();

    @Getter
    @AllArgsConstructor
    public static class Response<R> {
        private final ErrorOuterClass.Error error;
        private final R response;
    }

    protected final AtomicReference<S> stubRef = new AtomicReference<>();
    protected Set<Location> locations = new CopyOnWriteArraySet<>();

    protected S stub;

    public ServiceConnector(Set<Location> locations) {
        this.locations.addAll(locations);
    }

    public S getStub() {
        return stubRef.get();
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
                refresh(stub);
                continue;
            }
            try {
                Response<R> response = function.apply(stub);
                if (response.getError().getErrcodeValue() != 0) {
                    log.warn(
                        "Exec {} failed, code: [{}], message: {}.",
                        function.getClass(), response.error.getErrcode(), response.error.getErrmsg()
                    );
                    if (refreshCode.contains(response.getError().getErrcodeValue())) {
                        throw new DingoClientException.InvalidRouteTableException(response.error.getErrmsg());
                    }
                    if (retryCheck.test(response.error)) {
                        refresh(stub);
                        continue;
                    }
                    throw new DingoClientException(response.getError().getErrcodeValue(), response.getError().getErrmsg());
                }
                return response;
            } catch (StatusRuntimeException e) {
                log.warn("Exec {} failed: {}.", function.getClass(), e.getMessage());
                refresh(stub);
            }
        }
        throw new RuntimeException("Retry attempts exhausted, failed to exec operation.");
    }

    public void refresh(S stub) {
        if (!refresh.compareAndSet(false, true)) {
            return;
        }
        try {
            if (!stubRef.compareAndSet(stub, null)) {
                return;
            }
            for (Location location : locations) {
                if (Optional.of(location)
                    .map(this::newChannel)
                    .map(wrap(this::transformToLeaderChannel))
                    .map(this::newStub)
                    .ifPresent(stubRef::set)
                    .isPresent()
                ) {
                    return;
                }
            }
        } finally {
            refresh.set(false);
        }
    }
    
    protected ManagedChannel newChannel(Location location) {
        try {
            return ChannelManager.getChannel(location);
        } catch (Exception e) {
            log.warn("Connect {}:{} error", location, e);
        }
        return null;
    } 

    protected ManagedChannel newChannel(String host, int port) {
        return newChannel(new Location(host, port));
    }

    protected abstract ManagedChannel transformToLeaderChannel(ManagedChannel channel);

    protected abstract S newStub(ManagedChannel channel);

    public Set<Location> getLocations() {
        return locations;
    }

}
