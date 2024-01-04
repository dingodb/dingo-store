package io.dingodb.sdk.service;

import io.dingodb.sdk.common.DingoClientException.ExhaustedRetryException;
import io.dingodb.sdk.common.utils.ErrorCodeUtils.Strategy;
import io.dingodb.sdk.service.entity.Message.Request;
import io.dingodb.sdk.service.entity.Message.Response;
import io.grpc.CallOptions;

public interface ServiceCallCycle<REQ extends Request, RES extends Response> {

    interface Before<REQ extends Request, RES extends Response> {
        default void before(REQ req, CallOptions options, long trace) { }
    }

    interface After<REQ extends Request, RES extends Response> {
        default void after(REQ req, RES res, CallOptions options, String remote, long trace) { }
    }

    interface RBefore<REQ extends Request, RES extends Response> {
        default void rBefore(REQ req, CallOptions options, String remote, long trace) { }
    }

    interface RAfter<REQ extends Request, RES extends Response> {
        default void rAfter(REQ req, RES res, CallOptions options, String remote, long trace) { }
    }

    interface RError<REQ extends Request, RES extends Response> {
        default void rError(REQ req, CallOptions options, String remote, long trace, String statusMessage) { }
    }

    interface OnErrRes<REQ extends Request, RES extends Response> {
        default Strategy onErrStrategy(
            Strategy strategy, int retry, int remain, REQ req, RES res, CallOptions options, String remote, long trace
        ) {
            return strategy;
        }
    }

    interface OnException<REQ extends Request, RES extends Response> {
        default void onException(REQ req, Exception exception, CallOptions options, String remote, long trace) { }
    }

    interface OnRetry<REQ extends Request, RES extends Response> {
        default void onRetry(REQ req, RES res, CallOptions options, String remote, long trace) { }
    }

    interface OnFailed<REQ extends Request, RES extends Response> {
        default void onFailed(REQ req, RES res, CallOptions options, String remote, long trace) { }
    }

    interface OnIgnore<REQ extends Request, RES extends Response> {
        default void onIgnore(REQ req, RES res, CallOptions options, String remote, long trace) { }
    }

    interface OnRefresh<REQ extends Request, RES extends Response> {
        default void onRefresh(REQ req, RES res, CallOptions options, String remote, long trace) { }
    }

    interface OnNonConnection<REQ extends Request, RES extends Response> {
        default void onNonConnection(REQ req, CallOptions options, long trace) { }
    }

    interface OnThrow<REQ extends Request, RES extends Response> {
        default void onThrow(REQ req, ExhaustedRetryException exception, CallOptions options, long trace) { }
    }

}
