package io.dingodb.sdk.service;

import io.dingodb.sdk.service.entity.Message;
import io.dingodb.sdk.service.entity.Message.Response;
import io.grpc.MethodDescriptor;

import java.util.function.Supplier;

public interface Caller<S> {
    <REQ extends Message, RES extends Response> RES call(MethodDescriptor<REQ, RES> method, Supplier<REQ> provider);

    <REQ extends Message, RES extends Response> RES call(MethodDescriptor<REQ, RES> method, REQ request);
}
