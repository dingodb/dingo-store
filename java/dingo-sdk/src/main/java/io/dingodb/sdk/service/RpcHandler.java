package io.dingodb.sdk.service;

import io.dingodb.sdk.service.entity.Message.Response;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;

import static io.dingodb.sdk.service.entity.Message.Request;

public interface RpcHandler<REQ extends Request, RES extends Response> {

    MethodDescriptor<REQ, RES> matchMethod();

    default void enter(REQ req, CallOptions options, String remote, long trace) {
    }

    default void before(REQ req, CallOptions options, String remote, long trace) {
    }

    default void after(REQ req, RES res, CallOptions options, String remote, long trace) {
    }


    default void onNonResponse(REQ req, CallOptions options, String remote, long trace, String statusMessage) {
    }
}
