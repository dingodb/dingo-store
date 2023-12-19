package io.dingodb.sdk.service.caller;

import io.dingodb.sdk.common.DingoClientException.ExhaustedRetryException;
import io.dingodb.sdk.common.utils.NoBreakFunctions;
import io.dingodb.sdk.service.entity.Message.Request;
import io.dingodb.sdk.service.entity.Message.Response;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

@Slf4j
@EqualsAndHashCode(onlyExplicitlyIncluded = true)
@AllArgsConstructor
public class ServiceHandler<REQ extends Request, RES extends Response> implements io.dingodb.sdk.service.ServiceHandler<REQ, RES> {


    @Slf4j
    public static final class Before { }

    @Slf4j
    public static final class After { }

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
    public static final class onThrow { }

    @EqualsAndHashCode.Include
    public final MethodDescriptor<REQ, RES> method;

    private final List<io.dingodb.sdk.service.ServiceHandler<REQ, RES>> handlers = new CopyOnWriteArrayList<>();

    public void addHandler(io.dingodb.sdk.service.ServiceHandler<REQ, RES> handler) {
        handlers.add(handler);
    }

    public void removeHandler(io.dingodb.sdk.service.ServiceHandler<REQ, RES> handler) {
        handlers.remove(handler);
    }

    @Override
    public MethodDescriptor<REQ, RES> matchMethod() {
        return method;
    }

    @Override
    public void before(long reqProviderIdentity, CallOptions options, long trace) {
        handlers.forEach(NoBreakFunctions.wrap(handler -> {
            handler.before(reqProviderIdentity, options, trace);
        }));
        if (Before.log.isDebugEnabled()) {
            Before.log.debug(
                "Service call [{}] enter on [{}], trace [{}], request: {}, options: {}",
                method.getFullMethodName(), System.currentTimeMillis(), trace, reqProviderIdentity, options
            );
        }
    }

    @Override
    public void after(REQ req, RES res, CallOptions options, String remote, long trace) {
        handlers.forEach(NoBreakFunctions.wrap(handler -> {
            handler.after(req, res, options, remote, trace);
        }));
        if (After.log.isDebugEnabled()) {
            After.log.debug(
                "Service call [{}:{}] after on [{}], trace [{}], request: {}, response: {}, options: {}",
                remote, method.getFullMethodName(), System.currentTimeMillis(), trace, req, res, options
            );
        }
    }

    @Override
    public void onException(REQ req, Exception ex, CallOptions options, String remote, long trace) {
        handlers.forEach(NoBreakFunctions.wrap(handler -> {
            handler.onException(req, ex, options, remote, trace);
        }));
        if (OnException.log.isDebugEnabled()) {
            OnException.log.debug(
                "Service call [{}:{}] exception on [{}], trace [{}], request: {}, ex: {}, options: {}",
                remote, method.getFullMethodName(), System.currentTimeMillis(), trace, req, ex, options, ex
            );
        }
    }

    @Override
    public void onRetry(REQ req, RES res, CallOptions options, String remote, long trace) {
        handlers.forEach(NoBreakFunctions.wrap(handler -> {
            handler.onRetry(req, res, options, remote, trace);
        }));
        if (OnRetry.log.isDebugEnabled()) {
            OnRefresh.log.debug(
                "Service call [{}:{}] need retry on [{}], trace [{}], request: {}, response: {}, options: {}",
                remote, method.getFullMethodName(), System.currentTimeMillis(), trace, req, res, options
            );
        }
    }

    @Override
    public void onFailed(REQ req, RES res, CallOptions options, String remote, long trace) {
        handlers.forEach(NoBreakFunctions.wrap(handler -> {
            handler.onFailed(req, res, options, remote, trace);
        }));
        if (OnFailed.log.isDebugEnabled()) {
            OnFailed.log.debug(
                "Service call [{}:{}] failed on [{}], trace [{}], request: {}, response: {}, options: {}",
                remote, method.getFullMethodName(), System.currentTimeMillis(), trace, req, res, options
            );
        }
    }

    @Override
    public void onIgnore(REQ req, RES res, CallOptions options, String remote, long trace) {
        handlers.forEach(NoBreakFunctions.wrap(handler -> {
            handler.onIgnore(req, res, options, remote, trace);
        }));
        if (OnIgnore.log.isDebugEnabled()) {
            OnIgnore.log.debug(
                "Service call [{}:{}] ignore error on [{}], trace [{}], request: {}, response: {}, options: {}",
                remote, method.getFullMethodName(), System.currentTimeMillis(), trace, req, res, options
            );
        }
    }

    @Override
    public void onRefresh(REQ req, RES res, CallOptions options, String remote, long trace) {
        handlers.forEach(NoBreakFunctions.wrap(handler -> {
            handler.onRefresh(req, res, options, remote, trace);
        }));
        if (OnRefresh.log.isDebugEnabled()) {
            OnRefresh.log.debug(
                "Service call [{}:{}] need refresh on [{}], trace [{}], request: {}, response: {}, options: {}",
                remote, method.getFullMethodName(), System.currentTimeMillis(), trace, req, res, options
            );
        }
    }

    @Override
    public void onNonConnection(REQ req, CallOptions options, long trace) {
        handlers.forEach(NoBreakFunctions.wrap(handler -> {
            handler.onNonConnection(req, options, trace);
        }));
        if (OnNonConnection.log.isDebugEnabled()) {
            OnNonConnection.log.debug(
                "Service call [{}] non connection on [{}], trace [{}], request: {}, options: {}",
                method.getFullMethodName(), System.currentTimeMillis(), trace, req, options
            );
        }
    }

    @Override
    public void onThrow(REQ req, ExhaustedRetryException ex, CallOptions options, long trace) {
        handlers.forEach(NoBreakFunctions.wrap(handler -> {
            handler.onThrow(req, ex, options, trace);
        }));
        if (onThrow.log.isDebugEnabled()) {
            onThrow.log.debug(
                "Service call [{}] throw ex on [{}], trace [{}], request: {}, options: {}, message: {}",
                method.getFullMethodName(), System.currentTimeMillis(), trace, req, options, ex.getMessage()
            );
        }
    }
}
