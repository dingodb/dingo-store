package io.dingodb.sdk.service;

import io.dingodb.sdk.common.DingoClientException.ExhaustedRetryException;
import io.dingodb.sdk.common.utils.ErrorCodeUtils;
import io.dingodb.sdk.service.Caller.CallExecutor;
import io.dingodb.sdk.service.ServiceCallCycle.After;
import io.dingodb.sdk.service.ServiceCallCycle.Before;
import io.dingodb.sdk.service.ServiceCallCycle.OnErrRes;
import io.dingodb.sdk.service.ServiceCallCycle.OnException;
import io.dingodb.sdk.service.ServiceCallCycle.OnFailed;
import io.dingodb.sdk.service.ServiceCallCycle.OnIgnore;
import io.dingodb.sdk.service.ServiceCallCycle.OnNonConnection;
import io.dingodb.sdk.service.ServiceCallCycle.OnRefresh;
import io.dingodb.sdk.service.ServiceCallCycle.OnRetry;
import io.dingodb.sdk.service.ServiceCallCycle.OnThrow;
import io.dingodb.sdk.service.ServiceCallCycle.RAfter;
import io.dingodb.sdk.service.ServiceCallCycle.RBefore;
import io.dingodb.sdk.service.ServiceCallCycle.RError;
import io.dingodb.sdk.service.caller.RpcCaller;
import io.dingodb.sdk.service.entity.Message.Request;
import io.dingodb.sdk.service.entity.Message.Response;
import io.dingodb.sdk.service.entity.common.RequestInfo;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.MethodDescriptor;
import lombok.EqualsAndHashCode;
import lombok.experimental.Delegate;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.List;

import static io.dingodb.sdk.service.JsonMessageUtils.toJson;

@Slf4j
@EqualsAndHashCode(onlyExplicitlyIncluded = true)
public class ServiceCallCycles<REQ extends Request, RES extends Response>
    implements ServiceCallCycle<REQ, RES>, Before<REQ, RES>, After<REQ, RES>,
               OnException<REQ, RES>, OnRetry<REQ, RES>, OnIgnore<REQ, RES>, OnRefresh<REQ, RES>, OnThrow<REQ, RES>,
               OnNonConnection<REQ, RES>, OnErrRes<REQ, RES>, OnFailed<REQ, RES>,
               RBefore<REQ, RES>, RAfter<REQ, RES>, RError<REQ, RES>, CallExecutor<REQ, RES> {

    static class DelegateList<T> {
        private final List<T> list = new ArrayList<>();
        public synchronized void addListener(T t) {
            list.add(t);
        }
        public synchronized void removeListener(T t) {
            list.remove(t);
        }
    }

    static class DelegateReference<T> {
        public final T defaultRef;
        private T ref;

        DelegateReference(T defaultRef) {
            this.defaultRef = defaultRef;
            this.ref = defaultRef;
        }

        public synchronized void setHandler(T ref) {
            if (ref == null) {
                ref = defaultRef;
            }
            this.ref = ref;
        }
    }

    @Slf4j
    public static final class Before { }

    @Slf4j
    public static final class After { }

    @Slf4j
    public static final class OnErrRes { }

    @Slf4j
    public static final class OnException { }

    @Slf4j
    public static final class OnRetry { }

    @Slf4j
    public static final class OnFailed { }

    @Slf4j
    public static final class OnIgnore { }

    @Slf4j
    public static final class OnRefresh { }

    @Slf4j
    public static final class OnNonConnection { }

    @Slf4j
    public static final class OnThrow { }

    @Slf4j
    public static final class RBefore { }

    @Slf4j
    public static final class RAfter { }

    @Slf4j
    public static final class RError { }

    @Delegate
    private final DelegateList<ServiceCallCycle.Before<REQ, RES>> before = new DelegateList<>();
    @Delegate
    private final DelegateList<ServiceCallCycle.After<REQ, RES>> after = new DelegateList<>();
    @Delegate
    private final DelegateList<ServiceCallCycle.OnException<REQ, RES>> onException = new DelegateList<>();
    @Delegate
    private final DelegateList<ServiceCallCycle.OnRetry<REQ, RES>> onRetry = new DelegateList<>();
    @Delegate
    private final DelegateList<ServiceCallCycle.OnIgnore<REQ, RES>> onIgnore = new DelegateList<>();
    @Delegate
    private final DelegateList<ServiceCallCycle.OnRefresh<REQ, RES>> onRefresh = new DelegateList<>();
    @Delegate
    private final DelegateList<ServiceCallCycle.OnThrow<REQ, RES>> onThrow = new DelegateList<>();
    @Delegate
    private final DelegateList<ServiceCallCycle.OnNonConnection<REQ, RES>> onNonConnection = new DelegateList<>();
    @Delegate
    private final DelegateList<ServiceCallCycle.OnFailed<REQ, RES>> onFailed = new DelegateList<>();

    @Delegate
    private final DelegateList<ServiceCallCycle.RBefore<REQ, RES>> rBefore = new DelegateList<>();
    @Delegate
    private final DelegateList<ServiceCallCycle.RAfter<REQ, RES>> rAfter = new DelegateList<>();
    @Delegate
    private final DelegateList<ServiceCallCycle.RError<REQ, RES>> rError = new DelegateList<>();

    @Delegate
    private final DelegateReference<ServiceCallCycle.OnErrRes<REQ, RES>> onErrRes = new DelegateReference<>(null);
    @Delegate
    private final DelegateReference<CallExecutor<REQ, RES>> callExecutor = new DelegateReference<>(RpcCaller::call);

    @EqualsAndHashCode.Include
    public final MethodDescriptor<REQ, RES> method;

    public final Logger logger;

    public ServiceCallCycles(MethodDescriptor<REQ, RES> method, Logger logger) {
        this.method = method;
        this.logger = logger;
    }

    @Override
    public RES call(
        MethodDescriptor<REQ, RES> method,
        REQ request,
        CallOptions options,
        Channel channel,
        long trace,
        ServiceCallCycles<REQ, RES> handlers
    ) {
        return callExecutor.ref.call(method, request, options, channel, trace, handlers);
    }

    @Override
    public void before(REQ req, CallOptions options, long trace) {
        if (Before.log.isDebugEnabled() && logger.isDebugEnabled()) {
            logger.debug(
                toJson(method.getFullMethodName(), trace, req, null, options)
            );
        }
        req.setRequestInfo(RequestInfo.builder().requestId(trace).build());
        for (ServiceCallCycle.Before<REQ, RES> before : before.list) {
            before.before(req, options, trace);
        }
    }

    @Override
    public void rBefore(REQ req, CallOptions options, String remote, long trace) {
        if (RBefore.log.isDebugEnabled() && logger.isDebugEnabled()) {
            logger.debug(
                toJson(remote, method.getFullMethodName(), trace, req, null, options)
            );
        }
        for (ServiceCallCycle.RBefore<REQ, RES> rBefore : rBefore.list) {
            rBefore.rBefore(req, options, remote, trace);
        }
    }

    @Override
    public void after(REQ req, RES res, CallOptions options, String remote, long trace) {
        if (After.log.isDebugEnabled() && logger.isDebugEnabled()) {
            logger.debug(
                toJson(remote, method.getFullMethodName(), trace, req, res, options)
            );
        }
        for (ServiceCallCycle.After<REQ, RES> after : after.list) {
            after.after(req, res, options, remote, trace);
        }
    }

    @Override
    public void rAfter(REQ req, RES res, CallOptions options, String remote, long trace) {
        if (RAfter.log.isDebugEnabled() && logger.isDebugEnabled()) {
            logger.debug(
                toJson(remote, method.getFullMethodName(), trace, req, res, options)
            );
        }
        for (ServiceCallCycle.RAfter<REQ, RES> rAfter : rAfter.list) {
            rAfter.rAfter(req, res, options, remote, trace);
        }
    }

    @Override
    public void rError(REQ req, CallOptions options, String remote, long trace, String statusMessage) {
        if (RError.log.isDebugEnabled() && logger.isDebugEnabled()) {
            logger.debug(
                toJson(statusMessage, remote, method.getFullMethodName(), trace, req, null, options, null)
            );
        }
        for (ServiceCallCycle.RError<REQ, RES> rError : rError.list) {
            rError.rError(req, options, remote, trace, statusMessage);
        }
    }

    @Override
    public ErrorCodeUtils.Strategy onErrStrategy(
        ErrorCodeUtils.Strategy strategy,
        int retry,
        int remain,
        REQ req,
        RES res,
        CallOptions options,
        String remote,
        long trace
    ) {
        if (ServiceCallCycles.OnErrRes.log.isDebugEnabled() && logger.isDebugEnabled()) {
            logger.debug(
                "Service call [{}:{}] error on [{}], trace [{}], retry: {}, remain: {}, req: {}, res: {}, options: {}",
                remote, method.getFullMethodName(), System.currentTimeMillis(), trace, retry, remain, req, res, options
            );
        }
        ErrorCodeUtils.Strategy before = strategy;
        ServiceCallCycle.OnErrRes<REQ, RES> onErrRes = this.onErrRes.ref;
        if (onErrRes != null) {
            strategy = onErrRes.onErrStrategy(strategy, retry, remain, req, res, options, remote, trace);
        }
        if (before != strategy && ServiceCallCycles.OnErrRes.log.isDebugEnabled() && logger.isDebugEnabled()) {
            logger.debug(
                "Service call [{}:{}] error on [{}], trace [{}], before strategy [{}], change to [{}]",
                remote, method.getFullMethodName(), System.currentTimeMillis(), trace, before, strategy
            );
        }
        return strategy;
    }

    @Override
    public void onException(REQ req, Exception ex, CallOptions options, String remote, long trace) {
        if (OnException.log.isDebugEnabled() && logger.isDebugEnabled()) {
            logger.debug(
                toJson(null, remote, method.getFullMethodName(), trace, req, null, options, ex)
            );
        }
        for (ServiceCallCycle.OnException<REQ, RES> onException : onException.list) {
            onException.onException(req, ex, options, remote, trace);
        }
    }

    @Override
    public void onRetry(REQ req, RES res, CallOptions options, String remote, long trace) {
        if (OnRetry.log.isDebugEnabled() && logger.isDebugEnabled()) {
            logger.debug(
                toJson(remote, method.getFullMethodName(), trace, req, res, options)
            );
        }
        for (ServiceCallCycle.OnRetry<REQ, RES> onRetry : onRetry.list) {
            onRetry.onRetry(req, res, options, remote, trace);
        }
    }

    @Override
    public void onFailed(REQ req, RES res, CallOptions options, String remote, long trace) {
        if (OnFailed.log.isDebugEnabled() && logger.isDebugEnabled()) {
            logger.debug(
                toJson(remote, method.getFullMethodName(), trace, req, res, options)
            );
        }
        for (ServiceCallCycle.OnFailed<REQ, RES> onFailed : onFailed.list) {
            onFailed.onFailed(req, res, options, remote, trace);
        }
    }

    @Override
    public void onIgnore(REQ req, RES res, CallOptions options, String remote, long trace) {
        if (OnIgnore.log.isDebugEnabled() && logger.isDebugEnabled()) {
            logger.debug(
                toJson(remote, method.getFullMethodName(), trace, req, res, options)

            );
        }
        for (ServiceCallCycle.OnIgnore<REQ, RES> onIgnore : onIgnore.list) {
            onIgnore.onIgnore(req, res, options, remote, trace);
        }
    }

    @Override
    public void onRefresh(REQ req, RES res, CallOptions options, String remote, long trace) {
        if (OnRefresh.log.isDebugEnabled() && logger.isDebugEnabled()) {
            logger.debug(
                toJson(remote, method.getFullMethodName(), trace, req, res, options)
            );
        }
        for (ServiceCallCycle.OnRefresh<REQ, RES> onRefresh : onRefresh.list) {
            onRefresh.onRefresh(req, res, options, remote, trace);
        }
    }

    @Override
    public void onNonConnection(REQ req, CallOptions options, long trace) {
        if (OnNonConnection.log.isDebugEnabled() && logger.isDebugEnabled()) {
            logger.debug(
                toJson(method.getFullMethodName(), trace, req, null, options)
            );
        }
        for (ServiceCallCycle.OnNonConnection<REQ, RES> onNonConnection : onNonConnection.list) {
            onNonConnection.onNonConnection(req, options, trace);
        }
    }

    @Override
    public void onThrow(REQ req, ExhaustedRetryException ex, CallOptions options, long trace) {
        if (ServiceCallCycles.OnThrow.log.isDebugEnabled() && logger.isDebugEnabled()) {
            logger.debug(
                toJson(null, null, method.getFullMethodName(), trace, req, null, options, ex)
            );
        }
        for (ServiceCallCycle.OnThrow<REQ, RES> onThrow : onThrow.list) {
            onThrow.onThrow(req, ex, options, trace);
        }
    }
}
