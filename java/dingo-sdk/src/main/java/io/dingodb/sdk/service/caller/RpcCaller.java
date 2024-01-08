package io.dingodb.sdk.service.caller;

import io.dingodb.sdk.service.Caller;
import io.dingodb.sdk.service.Service;
import io.dingodb.sdk.service.ServiceCallCycles;
import io.dingodb.sdk.service.entity.Message;
import io.dingodb.sdk.service.entity.Message.Request;
import io.dingodb.sdk.service.entity.Message.Response;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.ClientCalls;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;

@Slf4j
@AllArgsConstructor
public class RpcCaller<S extends Service<S>> implements Caller<S>, InvocationHandler {

    @Getter
    private final Channel channel;
    @Getter
    private final CallOptions options;
    @Getter
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

    @Override
    public <REQ extends Request, RES extends Response> RES call(
        MethodDescriptor<REQ, RES> method, long requestId, REQ request, ServiceCallCycles<REQ, RES> handler
    ) {
        return call(method, request, options, channel, System.identityHashCode(request), handler);
    }

    protected static <REQ extends Message, RES extends Response> RpcFuture<RES> asyncCall(
        MethodDescriptor<REQ, RES> method, REQ request, CallOptions options, Channel channel
    ) {
        RpcFuture<RES> future = new RpcFuture<>();
        ClientCall<REQ, RES> call = channel.newCall(method, options);
        call.start(future.listener, new Metadata());
        call.request(2);
        call.sendMessage(request);
        call.halfClose();
        return future;
    }

    public static <REQ extends Request, RES extends Response> RES call(
        MethodDescriptor<REQ, RES> method,
        REQ request,
        CallOptions options,
        Channel channel,
        long trace,
        ServiceCallCycles<REQ, RES> handler
    ) {
        String methodName = method.getFullMethodName();

        if (channel == null) {
            handler.rBefore(request, options, null, trace);
            return null;
        }
        handler.rBefore(request, options, channel.authority(), trace);
        RES response;
        try {
            response = ClientCalls.blockingUnaryCall(channel, method, options, request);
        } catch (StatusRuntimeException e) {
            handler.rError(request, options, channel.authority(), trace, e.getMessage());
            return null;
        }

        handler.rAfter(request, response, options, channel.authority(), trace);
        return response;
    }

}
