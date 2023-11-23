package io.dingodb.sdk.service.caller;

import io.dingodb.sdk.common.utils.ErrorCodeUtils;
import io.dingodb.sdk.service.Caller;
import io.dingodb.sdk.service.ChannelProvider;
import io.dingodb.sdk.service.entity.Message;
import io.dingodb.sdk.service.entity.Message.Response;
import io.dingodb.sdk.service.Service;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.MethodDescriptor;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.ClientCalls;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.util.function.Supplier;

import static io.dingodb.sdk.common.utils.ErrorCodeUtils.Strategy.RETRY;

@Slf4j
@AllArgsConstructor
public class RpcCaller<S extends Service<S>> implements Caller<S>, InvocationHandler {

    private final ChannelProvider channelProvider;
    private final CallOptions options;
    private final S service;

    public RpcCaller(ChannelProvider channelProvider, CallOptions options, Class<S> genericClass) {
        this.channelProvider = channelProvider;
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
        Channel channel = channelProvider.channel();
        RES res = call(method, request, options, channel);
        if (res == null) {
            channelProvider.refresh(channel);
        }
        return res;
    }

    public static <REQ extends Message, RES extends Response> RES call(
        MethodDescriptor<REQ, RES> method, REQ request, CallOptions options, Channel channel
    ) {
        long traceId = System.identityHashCode(request);
        String methodName = method.getFullMethodName();
        if (log.isDebugEnabled()) {
            log.debug(
                "Call [{}], trace [{}], request: {}, options: {}", methodName, traceId, request, options
            );
        }
        if (channel == null) {
            log.debug(
                "Call [{}] channel is null, will refresh and retry, trace [{}], request: {}, options: {}", methodName, traceId, request, options
            );
            return null;
        }
        if (log.isDebugEnabled()) {
            log.debug(
                "Call [{}:{}] begin, trace [{}], request: {}, options: {}",
                channel.authority(), methodName, traceId, request, options
            );
        }
        RES response = null;
        try {
            response = ClientCalls.blockingUnaryCall(channel, method, options, request);
        } catch (StatusRuntimeException e) {
            if (log.isDebugEnabled()) {
                log.debug(
                    "Call [{}:{}] StatusRuntimeException [{}], trace [{}], request: {}, options: {}",
                    channel.authority(), methodName, e.getMessage(), traceId, request, options
                );
            }
        }
        if (log.isDebugEnabled()) {
            log.debug(
                "Call [{}:{}] finish, trace [{}], request: {}, response: {}, options: {}",
                channel.authority(), methodName, traceId, request, response, options
            );
        }
        if (response != null && response.getError() != null) {
            if (ErrorCodeUtils.errorToStrategy(response.getError().getErrcode().number()) == RETRY) {
                if (log.isDebugEnabled()) {
                    log.debug(
                        "Call [{}:{}] return retry code, will refresh, trace [{}], return code [{}]",
                        channel.authority(), methodName, traceId, response.getError().getErrmsg()
                    );
                }
                response = null;
            }
        }
        return response;
    }

}
