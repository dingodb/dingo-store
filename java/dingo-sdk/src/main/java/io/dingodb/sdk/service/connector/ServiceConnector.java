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

import io.dingodb.sdk.common.DingoClientException;
import io.dingodb.sdk.common.DingoClientException.InvalidRouteTableException;
import io.dingodb.sdk.common.DingoClientException.RequestErrorException;
import io.dingodb.sdk.common.DingoClientException.ExhaustedRetryException;
import io.dingodb.sdk.common.Location;
import io.dingodb.sdk.common.utils.ErrorCodeUtils.Strategy;
import io.dingodb.sdk.common.utils.NoBreakFunctions;
import io.dingodb.sdk.common.utils.Optional;
import io.dingodb.sdk.service.ChannelManager;
import io.grpc.Channel;
import io.grpc.ManagedChannel;
import io.grpc.stub.AbstractBlockingStub;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Function;
import java.util.stream.Collectors;

import static io.dingodb.error.ErrorOuterClass.Error;
import static io.dingodb.sdk.common.utils.ErrorCodeUtils.Strategy.FAILED;
import static io.dingodb.sdk.common.utils.ErrorCodeUtils.Strategy.IGNORE;
import static io.dingodb.sdk.common.utils.ErrorCodeUtils.Strategy.REFRESH;
import static io.dingodb.sdk.common.utils.ErrorCodeUtils.Strategy.RETRY;
import static io.dingodb.sdk.common.utils.ErrorCodeUtils.errorToStrategyFunc;
import static io.dingodb.sdk.common.utils.NoBreakFunctions.wrap;
import static io.dingodb.sdk.common.utils.StackTraces.CURRENT_STACK;
import static io.dingodb.sdk.common.utils.StackTraces.stack;

@Slf4j
public abstract class ServiceConnector<S extends AbstractBlockingStub<S>> {

    public static final int RETRY_TIMES = 30;
    private static Map<Class, ResponseBuilder> responseBuilders = new ConcurrentHashMap<>();

    @Getter
    @AllArgsConstructor
    public static class Response<R> {
        public final Error error;
        public final R response;
    }

    @AllArgsConstructor
    private static class ResponseBuilder<R> {
        private final Method errorGetter;

        public Response<R> build(R response) {
            try {
                return new Response<>((Error) errorGetter.invoke(response), response);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    protected final AtomicReference<S> stubRef = new AtomicReference<>();
    protected final Set<Location> locations = new CopyOnWriteArraySet<>();
    protected final int retryTimes;

    private final AtomicBoolean refresh = new AtomicBoolean();
    private boolean closed = false;

    @Deprecated
    public ServiceConnector(String locations) {
        this(locations, RETRY_TIMES);
    }

    @Deprecated
    public ServiceConnector(Set<Location> locations) {
        this.locations.addAll(locations);
        this.retryTimes = RETRY_TIMES;
    }

    public ServiceConnector(String locations, int retryTimes) {
        this(Optional.ofNullable(locations)
            .map(__ -> __.split(","))
            .map(Arrays::stream)
            .map(ss -> ss
                .map(s -> s.split(":"))
                .map(__ -> new Location(__[0], Integer.parseInt(__[1])))
                .collect(Collectors.toSet()))
            .orElseGet(Collections::emptySet), retryTimes);
    }

    public ServiceConnector(Set<Location> locations, int retryTimes) {
        this.locations.addAll(locations);
        this.retryTimes = retryTimes;
    }

    public void close() {
        closed = true;
    }

    public S getStub() {
        return stubRef.get();
    }

    <R> Response<R> toResponse(Object res) {
        return responseBuilders.computeIfAbsent(res.getClass(), NoBreakFunctions.<Class, ResponseBuilder>wrap(
            cls -> new ResponseBuilder<>(cls.getDeclaredMethod("getError")))
        ).build(res);
    }

    protected <R> R cleanResponse(Response<R> response) {
        return Optional.mapOrNull(response, Response::getResponse);
    }

    public <R> R exec(Function<S, R> function) {
        return cleanResponse(exec(stack(CURRENT_STACK + 1), function, RETRY_TIMES, errorToStrategyFunc, this::toResponse));
    }

    public <R> R exec(Function<S, R> function, int retryTimes) {
        return cleanResponse(exec(stack(CURRENT_STACK + 1), function, retryTimes, errorToStrategyFunc, this::toResponse));
    }

    public <R> R exec(Function<S, R> function, Function<Integer, Strategy> errChecker) {
        return cleanResponse(exec(stack(CURRENT_STACK + 1), function, RETRY_TIMES, errChecker, this::toResponse));
    }

    public <R> R exec(
        Function<S, R> function, int retryTimes, Function<Integer, Strategy> errChecker
    ) {
        return cleanResponse(exec(stack(CURRENT_STACK + 1), function, retryTimes, errChecker, this::toResponse));
    }

    public <R> Response<R> exec(
        Function<S, R> function, int retryTimes, Function<Integer, Strategy> errChecker, Function<R, Response<R>> toResponse
    ) {
        return exec(stack(CURRENT_STACK + 1), function, retryTimes, errChecker, toResponse);
    }

    public <R> R exec(
        String name,
        Function<S, R> task,
        int retryTimes,
        Function<Integer, Strategy> errChecker
    ) {
        return cleanResponse(exec(name, task, retryTimes, errChecker, this::toResponse));
    }

    public <R> Response<R> exec(
        String name,
        Function<S, R> task,
        int retryTimes,
        Function<Integer, Strategy> errChecker,
        Function<R, Response<R>> toResponse
    ) {
        if (closed) {
            throw new DingoClientException(-1, "The connector is closed, please check status.");
        }

        S stub = null;
        boolean connected = false;
        Map<String, Integer> errMsgs = new HashMap<>();

        while (retryTimes-- > 0) {
            try {
                if ((stub = getStub()) == null) {
                    LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(1));
                    refresh(stub);
                    continue;
                }

                connected = true;
                Response<R> response = toResponse.apply(task.apply(stub));
                Error error = response.getError();
                int errCode = error.getErrcodeValue();
                if (errCode != 0) {
                    String authority = Optional.mapOrGet(stub.getChannel(), Channel::authority, () -> "");
                    errMsgs.compute(authority + ">>" + error.getErrmsg(), (k, v) -> v == null ? 1 : v + 1);
                    switch (errChecker.apply(errCode)) {
                        case RETRY:
                            errorLog(name, authority, error, RETRY);
                            LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(1));
                            refresh(stub);
                            continue;
                        case FAILED:
                            errorLog(name, authority, error, FAILED);
                            throw new RequestErrorException(errCode, error.getErrmsg());
                        case REFRESH:
                            errorLog(name, authority, error, REFRESH);
                            LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(1));
                            refresh(stub);
                            throw new InvalidRouteTableException(response.error.getErrmsg());
                        case IGNORE:
                            errorLog(name, authority, error, IGNORE);
                            return null;
                        default:
                            throw new IllegalStateException("Unexpected value: " + errChecker.apply(errCode));
                    }
                }
                return response;
            } catch (Exception e) {
                if (e instanceof RequestErrorException || e instanceof InvalidRouteTableException) {
                    throw e;
                }
                if (log.isDebugEnabled()) {
                    log.warn("Exec {} failed: {}.", name, e.getMessage());
                }
                errMsgs.compute(e.getMessage(), (k, v) -> v == null ? 1 : v + 1);
                LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(1));
                refresh(stub);
            }
        }

        throw generateException(name, connected, errMsgs);
    }

    private <R> RuntimeException generateException(String name, boolean connected, Map<String, Integer> errMsgs) {
        // if connected is false, means can not get leader connection
        if (connected) {
            StringBuilder errMsgBuilder = new StringBuilder();
            errMsgBuilder.append("task: ").append(name).append("==>>");
            errMsgs.forEach((k, v) -> errMsgBuilder
                .append('[').append(v).append("] times [").append(k).append(']').append(", ")
            );
            throw new ExhaustedRetryException(
                "Exec attempts exhausted, failed to exec " + name + ", " + errMsgBuilder
            );
        } else {
            throw new ExhaustedRetryException(
                "Exec " + name + " error, " + "transform leader attempts exhausted."
            );
        }
    }

    private void errorLog(String name, String remote, Error error, Strategy strategy) {
        if (log.isDebugEnabled()) {
            log.warn(
                "Exec {} failed, remote: [{}], code: [{}], message: {}, strategy: {}.",
                name, remote, error.getErrcode(), error.getErrmsg(), strategy
            );
        }
    }

    public void refresh(S stub) {
        if (!refresh.compareAndSet(false, true)) {
            return;
        }
        try {
            if (!stubRef.compareAndSet(stub, null)) {
                return;
            }
            if (locations.isEmpty()) {
                Optional.ofNullable(this.transformToLeaderChannel(null))
                    .map(this::newStub)
                    .ifPresent(stubRef::set);
                return;
            }

            for (Location location : locations) {
                if (Optional.of(location)
                    .map(this::newChannel)
                    .map(wrap(this::transformToLeaderChannel))
                    .map(this::newStub)
                    .ifPresent(stubRef::set)
                    .isPresent()) {
                    return;
                }
            }
        } catch (Exception e) {
            if (log.isDebugEnabled()) {
                log.warn("Get connection stub failed, will retry...");
            }
        } finally {
            refresh.set(false);
        }
    }
    
    protected ManagedChannel newChannel(Location location) {
        try {
            return ChannelManager.getChannel(location);
        } catch (Exception e) {
            log.warn("Connect {} error", location, e);
        }
        return null;
    } 

    protected ManagedChannel newChannel(String host, int port) {
        return newChannel(new Location(host, port));
    }

    protected abstract ManagedChannel transformToLeaderChannel(ManagedChannel channel);

    protected abstract S newStub(ManagedChannel channel);

}
