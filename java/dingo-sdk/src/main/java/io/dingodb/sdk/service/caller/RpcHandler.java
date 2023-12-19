package io.dingodb.sdk.service.caller;

import io.dingodb.sdk.common.utils.NoBreakFunctions;
import io.dingodb.sdk.service.entity.common.RequestInfo;
import io.grpc.CallOptions;
import io.grpc.MethodDescriptor;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static io.dingodb.sdk.service.entity.Message.Request;
import static io.dingodb.sdk.service.entity.Message.Response;

@Slf4j
@EqualsAndHashCode(onlyExplicitlyIncluded = true)
@AllArgsConstructor
public class RpcHandler<REQ extends Request, RES extends Response> implements io.dingodb.sdk.service.RpcHandler<REQ, RES> {

    @Slf4j
    public static final class Enter { }

    @Slf4j
    public static final class Before { }

    @Slf4j
    public static final class After { }

    @Slf4j
    public static final class OnNonResponse { }

    @EqualsAndHashCode.Include
    public final MethodDescriptor<REQ, RES> method;

    private final List<io.dingodb.sdk.service.RpcHandler<REQ, RES>> handlers = new CopyOnWriteArrayList<>();

    public void addHandler(io.dingodb.sdk.service.RpcHandler<REQ, RES> handler) {
        handlers.add(handler);
    }

    public void removeHandler(io.dingodb.sdk.service.RpcHandler<REQ, RES> handler) {
        handlers.remove(handler);
    }

    @Override
    public MethodDescriptor<REQ, RES> matchMethod() {
        return method;
    }

    @Override
    public void enter(REQ req, CallOptions options, String remote, long trace) {
        handlers.forEach(NoBreakFunctions.wrap(handler -> {
            handler.enter(req, options, remote, trace);
        }));
        if (Enter.log.isDebugEnabled()) {
            Enter.log.debug(
                "Call [{}:{}] enter on [{}], trace [{}], request: {}, options: {}",
                remote, method.getFullMethodName(), System.currentTimeMillis(), trace, req, options
            );
        }
        req.setRequestInfo(new RequestInfo(trace));
    }

    @Override
    public void before(REQ req, CallOptions options, String remote, long trace) {
        handlers.forEach(NoBreakFunctions.wrap(handler -> {
            handler.before(req, options, remote, trace);
        }));
        if (Before.log.isDebugEnabled()) {
            Before.log.debug(
                "Call [{}:{}] before on [{}], trace [{}], request: {}, options: {}",
                method.getFullMethodName(), remote, System.currentTimeMillis(), trace, req, options
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
                "Call [{}:{}] after on {}, trace [{}], request: {}, response: {}, options: {}",
                remote, method.getFullMethodName(), System.currentTimeMillis(), trace, req, res, options
            );
        }
    }

    @Override
    public void onNonResponse(REQ req, CallOptions options, String remote, long trace, String statusMessage) {
        handlers.forEach(NoBreakFunctions.wrap(handler -> {
            handler.onNonResponse(req, options, remote, trace, statusMessage);
        }));
        if (OnNonResponse.log.isDebugEnabled()) {
            OnNonResponse.log.debug(
                "Call [{}:{}] error on {}, message [{}], trace [{}], request: {}, options: {}",
                remote, method.getFullMethodName(), System.currentTimeMillis(), statusMessage, trace, req, options
            );
        }
    }
}
