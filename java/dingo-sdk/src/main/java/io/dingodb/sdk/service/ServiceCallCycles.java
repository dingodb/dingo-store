package io.dingodb.sdk.service;

import io.dingodb.sdk.common.DingoClientException.ExhaustedRetryException;
import io.dingodb.sdk.common.utils.ErrorCodeUtils;
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
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.experimental.Delegate;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;

@Slf4j
@EqualsAndHashCode(onlyExplicitlyIncluded = true)
public class ServiceCallCycles<REQ extends Request, RES extends Response>
    implements ServiceCallCycle<REQ, RES>, Before<REQ, RES>, After<REQ, RES>,
               OnException<REQ, RES>, OnRetry<REQ, RES>, OnIgnore<REQ, RES>, OnRefresh<REQ, RES>, OnThrow<REQ, RES>,
               OnNonConnection<REQ, RES>, OnErrRes<REQ, RES>, OnFailed<REQ, RES>,
               RBefore<REQ, RES>, RAfter<REQ, RES>, RError<REQ, RES> {

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
        }

        public synchronized void setHandler(T ref) {
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
    private final DelegateReference<Caller.CallExecutor> callExecutor = new DelegateReference<>(RpcCaller::call);

    @EqualsAndHashCode.Include
    public final MethodDescriptor<REQ, RES> method;

    public ServiceCallCycles(MethodDescriptor<REQ, RES> method) {
        this.method = method;
    }

    @Override
    public void before(REQ req, CallOptions options, long trace) {
        if (Before.log.isDebugEnabled()) {
            Before.log.debug(
                "Service call [{}] enter on [{}], trace [{}], request: {}, options: {}",
                method.getFullMethodName(), System.currentTimeMillis(), trace, req, options
            );
        }
        for (ServiceCallCycle.Before<REQ, RES> before : before.list) {
            before.before(req, options, trace);
        }
    }

    @Override
    public void rBefore(REQ req, CallOptions options, String remote, long trace) {
        if (RBefore.log.isDebugEnabled()) {
            RBefore.log.debug(
                "RCall [{}:{}] enter on [{}], trace [{}], request: {}, options: {}",
                req, method.getFullMethodName(), System.currentTimeMillis(), trace, req, options
            );
        }
        for (ServiceCallCycle.RBefore<REQ, RES> rBefore : rBefore.list) {
            rBefore.rBefore(req, options, remote, trace);
        }
    }

    @Override
    public void after(REQ req, RES res, CallOptions options, String remote, long trace) {
        if (After.log.isDebugEnabled()) {
            After.log.debug(
                "Service call [{}:{}] after on [{}], trace [{}], request: {}, response: {}, options: {}",
                remote, method.getFullMethodName(), System.currentTimeMillis(), trace, req, res, options
            );
        }
        for (ServiceCallCycle.After<REQ, RES> after : after.list) {
            after.after(req, res, options, remote, trace);
        }
    }

    @Override
    public void rAfter(REQ req, RES res, CallOptions options, String remote, long trace) {
        if (RAfter.log.isDebugEnabled()) {
            RAfter.log.debug(
                "RCall [{}:{}] after on [{}], trace [{}], request: {}, response: {}, options: {}",
                remote, method.getFullMethodName(), System.currentTimeMillis(), trace, req, res, options
            );
        }
        for (ServiceCallCycle.RAfter<REQ, RES> rAfter : rAfter.list) {
            rAfter.rAfter(req, res, options, remote, trace);
        }
    }

    @Override
    public void rError(REQ req, CallOptions options, String remote, long trace, String statusMessage) {
        if (RError.log.isDebugEnabled()) {
            RError.log.debug(
                "RCall [{}:{}] error on {}, message [{}], trace [{}], request: {}, options: {}",
                remote, method.getFullMethodName(), System.currentTimeMillis(), statusMessage, trace, req, options
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
        if (ServiceCallCycles.OnErrRes.log.isDebugEnabled()) {
            OnErrRes.log.debug(
                "Service call [{}:{}] error on [{}], trace [{}], retry: {}, remain: {}, req: {}, res: {}, options: {}",
                remote, method.getFullMethodName(), System.currentTimeMillis(), trace, retry, remain, req, res, options
            );
        }
        ServiceCallCycle.OnErrRes<REQ, RES> onErrRes = this.onErrRes.ref;
        if (onErrRes != null) {
            strategy = onErrRes.onErrStrategy(strategy, retry, remain, req, res, options, remote, trace);
        }
        return strategy;
    }

    @Override
    public void onException(REQ req, Exception ex, CallOptions options, String remote, long trace) {
        if (OnException.log.isDebugEnabled()) {
            OnException.log.debug(
                "Service call [{}:{}] exception on [{}], trace [{}], request: {}, ex: {}, options: {}",
                remote, method.getFullMethodName(), System.currentTimeMillis(), trace, req, ex, options, ex
            );
        }
        for (ServiceCallCycle.OnException<REQ, RES> onException : onException.list) {
            onException.onException(req, ex, options, remote, trace);
        }
    }

    @Override
    public void onRetry(REQ req, RES res, CallOptions options, String remote, long trace) {
        if (OnRetry.log.isDebugEnabled()) {
            OnRefresh.log.debug(
                "Service call [{}:{}] need retry on [{}], trace [{}], request: {}, response: {}, options: {}",
                remote, method.getFullMethodName(), System.currentTimeMillis(), trace, req, res, options
            );
        }
        for (ServiceCallCycle.OnRetry<REQ, RES> onRetry : onRetry.list) {
            onRetry.onRetry(req, res, options, remote, trace);
        }
    }

    @Override
    public void onFailed(REQ req, RES res, CallOptions options, String remote, long trace) {
        if (OnFailed.log.isDebugEnabled()) {
            OnFailed.log.debug(
                "Service call [{}:{}] failed on [{}], trace [{}], request: {}, response: {}, options: {}",
                remote, method.getFullMethodName(), System.currentTimeMillis(), trace, req, res, options
            );
        }
        for (ServiceCallCycle.OnFailed<REQ, RES> onFailed : onFailed.list) {
            onFailed.onFailed(req, res, options, remote, trace);
        }
    }

    @Override
    public void onIgnore(REQ req, RES res, CallOptions options, String remote, long trace) {
        if (OnIgnore.log.isDebugEnabled()) {
            OnIgnore.log.debug(
                "Service call [{}:{}] ignore error on [{}], trace [{}], request: {}, response: {}, options: {}",
                remote, method.getFullMethodName(), System.currentTimeMillis(), trace, req, res, options
            );
        }
        for (ServiceCallCycle.OnIgnore<REQ, RES> onIgnore : onIgnore.list) {
            onIgnore.onIgnore(req, res, options, remote, trace);
        }
    }

    @Override
    public void onRefresh(REQ req, RES res, CallOptions options, String remote, long trace) {
        if (OnRefresh.log.isDebugEnabled()) {
            OnRefresh.log.debug(
                "Service call [{}:{}] need refresh on [{}], trace [{}], request: {}, response: {}, options: {}",
                remote, method.getFullMethodName(), System.currentTimeMillis(), trace, req, res, options
            );
        }
        for (ServiceCallCycle.OnRefresh<REQ, RES> onRefresh : onRefresh.list) {
            onRefresh.onRefresh(req, res, options, remote, trace);
        }
    }

    @Override
    public void onNonConnection(REQ req, CallOptions options, long trace) {
        if (OnNonConnection.log.isDebugEnabled()) {
            OnNonConnection.log.debug(
                "Service call [{}] non connection on [{}], trace [{}], request: {}, options: {}",
                method.getFullMethodName(), System.currentTimeMillis(), trace, req, options
            );
        }
        for (ServiceCallCycle.OnNonConnection<REQ, RES> onNonConnection : onNonConnection.list) {
            onNonConnection.onNonConnection(req, options, trace);
        }
    }

    @Override
    public void onThrow(REQ req, ExhaustedRetryException ex, CallOptions options, long trace) {
        if (ServiceCallCycles.OnThrow.log.isDebugEnabled()) {
            ServiceCallCycles.OnThrow.log.debug(
                "Service call [{}] throw ex on [{}], trace [{}], request: {}, options: {}, message: {}",
                method.getFullMethodName(), System.currentTimeMillis(), trace, req, options, ex.getMessage()
            );
        }
        for (ServiceCallCycle.OnThrow<REQ, RES> onThrow : onThrow.list) {
            onThrow.onThrow(req, ex, options, trace);
        }
    }
}
