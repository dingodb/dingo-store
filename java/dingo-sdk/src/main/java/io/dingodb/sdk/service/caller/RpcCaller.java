package io.dingodb.sdk.service.caller;

import io.dingodb.sdk.common.utils.ErrorCodeUtils;
import io.dingodb.sdk.service.Caller;
import io.dingodb.sdk.service.Service;
import io.dingodb.sdk.service.entity.Message;
import io.dingodb.sdk.service.entity.Message.Response;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.ClientCalls;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.function.Supplier;

import static io.dingodb.sdk.common.utils.ErrorCodeUtils.Strategy.RETRY;

@Slf4j
@AllArgsConstructor
public class RpcCaller<S extends Service<S>> implements Caller<S>, InvocationHandler {

    private final Channel channel;
    private final CallOptions options;
    private final S service;

    public RpcCaller(Channel channel, CallOptions options, Class<S> genericClass) {
        this.channel = channel;
        this.options = options;
        this.service = proxy(genericClass);
    }

    private S proxy(Class<S> genericType) {
        for (Class<?> child : genericType.getClasses()) {
            if (!child.getSuperclass().equals(genericType)) {
                try {
                    return (S) child.getConstructor(Caller.class).newInstance(this);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        }
        throw new RuntimeException("Not found " + genericType.getName() + " impl.");
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        return method.invoke(service, args);
    }

    public <REQ extends Message, RES extends Response> RES call(MethodDescriptor<REQ, RES> method, Supplier<REQ> provider) {
        return call(method, provider.get());
    }

    public <REQ extends Message, RES extends Response> RES call(MethodDescriptor<REQ, RES> method, REQ request) {
        return call(method, request, options, channel, System.identityHashCode(request));
    }

    public static <REQ extends Message, RES extends Response> RpcFuture<RES> asyncCall(
        MethodDescriptor<REQ, RES> method, REQ request, CallOptions options, Channel channel, long trace
    ) {
        String methodName = method.getFullMethodName();
        RpcFuture<RES> future = new RpcFuture<>();

        if (log.isDebugEnabled()) {
            log.debug(
                "Call [{}], trace [{}], request: {}, options: {}", methodName, trace, request, options
            );
        }
        if (channel == null) {
            log.debug(
                "Call [{}] channel is null, will refresh and retry, trace [{}], request: {}, options: {}",
                methodName, trace, request, options
            );
            future.complete(null);
            return future;
        }
        ClientCall<REQ, RES> call = channel.newCall(method, options);
        call.start(future.listener, new Metadata());
        call.request(2);
        call.sendMessage(request);
        call.halfClose();
        return future;
    }

    public static <REQ extends Message, RES extends Response> RES call(
        MethodDescriptor<REQ, RES> method, REQ request, CallOptions options, Channel channel, long trace
    ) {
        String methodName = method.getFullMethodName();
        if (log.isDebugEnabled()) {
            log.debug(
                "Call [{}], trace [{}], request: {}, options: {}", methodName, trace, request, options
            );
        }
        if (channel == null) {
            log.debug(
                "Call [{}] channel is null, will refresh and retry, trace [{}], request: {}, options: {}",
                methodName, trace, request, options
            );
            return null;
        }
        long start = System.currentTimeMillis();
        if (log.isDebugEnabled()) {
            log.debug(
                "Call [{}:{}] begin, trace [{}], request: {}, options: {}",
                channel.authority(), methodName, trace, request, options
            );
        }
        RES response = null;
        try {
            response = ClientCalls.blockingUnaryCall(channel, method, options, request);
        } catch (StatusRuntimeException e) {
            if (log.isDebugEnabled()) {
                log.debug(
                    "Call [{}:{}] StatusRuntimeException [{}], trace [{}], request: {}, options: {}",
                    channel.authority(), methodName, e.getMessage(), trace, request, options
                );
            }
        }
        if (log.isDebugEnabled()) {
            log.debug(
                "Call [{}:{}] finish, use [{}] ms, trace [{}], request: {}, response: {}, options: {}",
                channel.authority(), methodName, System.currentTimeMillis() - start, trace, request, response, options
            );
        }
        if (response != null && response.getError() != null) {
            if (ErrorCodeUtils.errorToStrategy(response.getError().getErrcode().number()) == RETRY) {
                if (log.isDebugEnabled()) {
                    log.debug(
                        "Call [{}:{}] return retry code, will refresh, trace [{}], return code [{}]",
                        channel.authority(), methodName, trace, response.getError().getErrmsg()
                    );
                }
                response = null;
            }
        }
        return response;
    }

}
