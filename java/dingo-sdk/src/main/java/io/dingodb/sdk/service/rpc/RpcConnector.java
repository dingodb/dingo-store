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

package io.dingodb.sdk.service.rpc;

import io.dingodb.sdk.common.DingoClientException;
import io.dingodb.sdk.common.DingoClientException.InvalidRouteTableException;
import io.dingodb.sdk.common.DingoClientException.RequestErrorException;
import io.dingodb.sdk.common.DingoClientException.RetryException;
import io.dingodb.sdk.common.utils.ErrorCodeUtils.Strategy;
import io.dingodb.sdk.common.utils.Optional;
import io.dingodb.sdk.common.utils.ReflectionUtils;
import io.dingodb.sdk.service.rpc.message.common.Location;
import io.dingodb.sdk.service.rpc.message.error.Error;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.stub.ClientCalls;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Function;
import java.util.function.Supplier;

import static io.dingodb.sdk.common.utils.ErrorCodeUtils.Strategy.FAILED;
import static io.dingodb.sdk.common.utils.ErrorCodeUtils.Strategy.IGNORE;
import static io.dingodb.sdk.common.utils.ErrorCodeUtils.Strategy.REFRESH;
import static io.dingodb.sdk.common.utils.ErrorCodeUtils.Strategy.RETRY;
import static io.dingodb.sdk.common.utils.ErrorCodeUtils.defaultCodeChecker;
import static io.dingodb.sdk.common.utils.NoBreakFunctions.wrap;
import static java.lang.reflect.Proxy.newProxyInstance;

@Slf4j
public class RpcConnector<S extends Service<S>> {

    public static final int RETRY_TIMES = 30;
    private static final ThreadLocal<Map<String, Integer>> ERR_MSGS = ThreadLocal.withInitial(HashMap::new);

    private final Class genericType = ReflectionUtils.getGenericType(this.getClass(), 0);
    protected Function<Channel, Channel> leaderChannelProvider;

    protected final Set<Location> locations = new CopyOnWriteArraySet<>();
    protected final int retryTimes;

    private final AtomicReference<Channel> channelRef = new AtomicReference<>();
    private final AtomicBoolean refresh = new AtomicBoolean();
    private boolean closed = false;

    protected RpcConnector(Set<Location> locations, int retryTimes, Function<Channel, Channel> leaderChannelProvider) {
        this.leaderChannelProvider = leaderChannelProvider;
        this.locations.addAll(locations);
        this.retryTimes = retryTimes;
    }

    public void close() {
        closed = true;
    }

    public S createCaller() {
        return (S) newProxyInstance(getClass().getClassLoader(), new Class[] {genericType}, new RpcCaller<>(this, genericType));
    }

    public Function<Channel, Channel> leaderChannelProvider() {
        return leaderChannelProvider;
    }

    protected <REQ extends Message, RES extends Message.Response> RpcFuture<RES> callAsync(MethodDescriptor<REQ, RES> method, REQ request, CallOptions options) {
        ClientCall<REQ, RES> call = channelRef.get().newCall(method, options);
        RpcFuture<RES> future = new RpcFuture<>();
        call.start(future.listener, new Metadata());
        call.sendMessage(request);
        call.halfClose();
        return future;
    }

    protected <REQ extends Message, RES extends Message.Response> RES call(
        MethodDescriptor<REQ, RES> method, Supplier<REQ> provider, CallOptions options
    ) {

        if (closed) {
            throw new DingoClientException(-1, "The connector is closed, please check status.");
        }
        int retryTimes = this.retryTimes;
        boolean connected = false;
        ERR_MSGS.get().clear();
        Channel channel = null;
        String serviceName = method.getServiceName();
        while (retryTimes-- > 0) {
            try {
                if ((channel = channelRef.get()) == null) {
                    if (log.isDebugEnabled()) {
                        log.debug(
                            "Call method: [{}], channel is null, refresh.",
                            method.getServiceName()
                        );
                    }
                    LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(1));
                    refresh(null);
                    continue;
                }

                connected = true;
                REQ request = provider.get();
                if (log.isDebugEnabled()) {
                    log.debug(
                        "Call method: [{}], request: {}, options: {}",
                        method.getServiceName(), request, options
                    );
                }
                RES response = ClientCalls.blockingUnaryCall(channel, method, options, request);
                if (response.error() != null && response.error().errcode().number() != 0) {
                    Error error = response.error();
                    int errCode = error.errcode().number();
                    String authority = channel.authority();
                    ERR_MSGS.get().compute(authority + ">>" + error.getErrmsg(), (k, v) -> v == null ? 1 : v + 1);
                    switch (defaultCodeChecker.apply(errCode)) {
                        case RETRY:
                            errorLog(serviceName, authority, error, RETRY);
                            LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(1));
                            refresh(channel);
                            continue;
                        case FAILED:
                            errorLog(serviceName, authority, error, FAILED);
                            throw new RequestErrorException(errCode, error.errmsg());
                        case REFRESH:
                            errorLog(serviceName, authority, error, REFRESH);
                            LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(1));
                            refresh(channel);
                            throw new InvalidRouteTableException(error.errmsg());
                        case IGNORE:
                            errorLog(serviceName, authority, error, IGNORE);
                            return null;
                        default:
                            throw new IllegalStateException("Unexpected value: " + defaultCodeChecker.apply(errCode));
                    }
                }
                return response;
            } catch (Exception e) {
                if (e instanceof RequestErrorException || e instanceof InvalidRouteTableException) {
                    throw e;
                }
                if (log.isDebugEnabled()) {
                    log.warn("Exec {} failed: {}.", serviceName, e.getMessage());
                }
                ERR_MSGS.get().compute(e.getMessage(), (k, v) -> v == null ? 1 : v + 1);
                LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(1));
                refresh(channel);
            }
        }

        throw generateException(serviceName, connected);
    }

    private RuntimeException generateException(String name, boolean connected) {
        // if connected is false, means can not get leader connection
        if (connected) {
            StringBuilder errMsgBuilder = new StringBuilder();
            errMsgBuilder.append("task: ").append(name).append("==>>");
            ERR_MSGS.get().forEach((k, v) -> errMsgBuilder
                .append('[').append(v).append("] times [").append(k).append(']').append(", ")
            );
            throw new RetryException(
                "Exec attempts exhausted, failed to exec " + name + ", " + errMsgBuilder
            );
        } else {
            throw new RetryException(
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

    protected void refresh(Channel channel) {
        if (!refresh.compareAndSet(false, true)) {
            return;
        }
        try {
            if (!channelRef.compareAndSet(channel, null)) {
                return;
            }

            if (locations.isEmpty()) {
                Optional.ofNullable(leaderChannelProvider.apply(null))
                    .ifPresent(channelRef::set);
                return;
            }

            for (Location location : locations) {
                if (Optional.of(location)
                    .map(this::newChannel)
                    .map(wrap(leaderChannelProvider::apply))
                    .ifPresent(channelRef::set)
                    .isPresent()
                ) {
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

}
