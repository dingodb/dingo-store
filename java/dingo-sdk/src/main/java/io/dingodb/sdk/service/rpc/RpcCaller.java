package io.dingodb.sdk.service.rpc;

import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.util.function.Supplier;

@Slf4j
@AllArgsConstructor
public class RpcCaller<S extends Service<S>> implements InvocationHandler {

    private final S service;
    private final RpcConnector<S> connector;
    private final CallOptions options;

    public RpcCaller(RpcConnector<S> connector, Class<S> genericClass) {
        this.connector = connector;
        this.options = CallOptions.DEFAULT;
        this.service = proxy(genericClass);
    }

    public RpcCaller(RpcConnector<S> connector, CallOptions options, Class<S> genericClass) {
        this.connector = connector;
        this.options = options;
        this.service = proxy(genericClass);
    }

    private S proxy(Class<S> genericType) {
        for (Class<?> child : genericType.getClasses()) {
            if (!child.getSuperclass().equals(genericType)) {
                try {
                    return (S) child.getConstructor(RpcCaller.class).newInstance(this);
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

    public <REQ extends Message, RES extends Message.Response> RES call(MethodDescriptor<REQ, RES> method, Supplier<REQ> provider) {
        return connector.call(method, provider, options);
    }

    public <REQ extends Message, RES extends Message.Response> RES call(MethodDescriptor<REQ, RES> method, REQ request) {
        return connector.call(method, () -> request, options);
    }

    public <REQ extends Message, RES extends Message.Response> RpcFuture<RES> callAsync(MethodDescriptor<REQ, RES> method, REQ request) {
        return connector.callAsync(method, request, options);
    }

}
