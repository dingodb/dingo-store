package io.dingodb.sdk.service;

import io.dingodb.sdk.service.entity.Message.Request;
import io.dingodb.sdk.service.entity.Message.Response;
import io.grpc.MethodDescriptor;

import java.util.function.Supplier;

public interface Caller<S> {
    <REQ extends Request, RES extends Response> RES call(
        MethodDescriptor<REQ, RES> method, Supplier<REQ> provider
    );

    <REQ extends Request, RES extends Response> RES call(
        MethodDescriptor<REQ, RES> method, REQ request
    );

    <REQ extends Request, RES extends Response> RES call(
        MethodDescriptor<REQ, RES> method, long requestId, Supplier<REQ> provider
    );

    <REQ extends Request, RES extends Response> RES call(
        MethodDescriptor<REQ, RES> method, long requestId, REQ request
    );
}
