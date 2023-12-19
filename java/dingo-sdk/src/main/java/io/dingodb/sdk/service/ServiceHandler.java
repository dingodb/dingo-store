package io.dingodb.sdk.service;

import io.dingodb.sdk.common.DingoClientException.ExhaustedRetryException;
import io.dingodb.sdk.service.entity.Message.Request;
import io.dingodb.sdk.service.entity.Message.Response;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;

public interface ServiceHandler<REQ extends Request, RES extends Response> {

    MethodDescriptor<REQ, RES> matchMethod();

    default void before(long reqProviderIdentity, CallOptions options, long trace) {
    }

    default void after(REQ req, RES res, CallOptions options, String remote, long trace) {
    }

    default void onException(REQ req, Exception exception, CallOptions options, String remote, long trace) {
    }

    default void onRetry(REQ req, RES res, CallOptions options, String remote, long trace) {
    }

    default void onFailed(REQ req, RES res, CallOptions options, String remote, long trace) {
    }

    default void onIgnore(REQ req, RES res, CallOptions options, String remote, long trace) {
    }

    default void onRefresh(REQ req, RES res, CallOptions options, String remote, long trace) {
    }

    default void onNonConnection(REQ req, CallOptions options, long trace) {
    }

    default void onThrow(REQ req, ExhaustedRetryException exception, CallOptions options, long trace) {
    }
}
