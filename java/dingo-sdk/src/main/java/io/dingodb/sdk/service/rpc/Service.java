package io.dingodb.sdk.service.rpc;

public interface Service<S extends Service<S>> {

    RpcCaller<S> getCaller();

}
