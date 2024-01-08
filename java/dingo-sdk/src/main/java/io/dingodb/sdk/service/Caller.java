package io.dingodb.sdk.service;

import io.dingodb.sdk.service.entity.Message.Request;
import io.dingodb.sdk.service.entity.Message.Response;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.MethodDescriptor;

public interface Caller<S> {

    interface CallExecutor<REQ extends Request, RES extends Response> {
        RES call(
            MethodDescriptor<REQ, RES> method,
            REQ request,
            CallOptions options,
            Channel channel,
            long trace,
            ServiceCallCycles<REQ, RES> handlers
        );
    }

    <REQ extends Request, RES extends Response> RES call(
        MethodDescriptor<REQ, RES> method, long requestId, REQ request, ServiceCallCycles<REQ, RES> handlers
    );
}
