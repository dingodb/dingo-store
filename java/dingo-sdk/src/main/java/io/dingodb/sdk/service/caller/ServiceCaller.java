package io.dingodb.sdk.service.caller;

import io.dingodb.sdk.common.DingoClientException.ExhaustedRetryException;
import io.dingodb.sdk.common.DingoClientException.InvalidRouteTableException;
import io.dingodb.sdk.common.DingoClientException.RequestErrorException;
import io.dingodb.sdk.service.Caller;
import io.dingodb.sdk.service.ChannelProvider;
import io.dingodb.sdk.service.Service;
import io.dingodb.sdk.service.ServiceCallCycles;
import io.dingodb.sdk.service.entity.Message.Request;
import io.dingodb.sdk.service.entity.Message.Response;
import io.dingodb.sdk.service.entity.error.Errno;
import io.dingodb.sdk.service.entity.error.Error;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.MethodDescriptor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Function;

import static io.dingodb.sdk.common.utils.ErrorCodeUtils.errorToStrategy;

@Slf4j
public class ServiceCaller<S extends Service<S>> implements Caller<S> {

    private int retry;
    private CallOptions options;

    private final ChannelProvider channelProvider;

    @Getter
    private final S service;

    public ServiceCaller(
        ChannelProvider channelProvider, int retry, CallOptions options, Function<Caller<S>, S> serviceFactory
    ) {
        this.channelProvider = channelProvider;
        this.retry = retry;
        this.options = options;
        this.service = serviceFactory.apply(this);
    }

    public int retry() {
        return retry;
    }

    public ServiceCaller<S> retry(int retry) {
        this.retry = retry;
        return this;
    }

    public CallOptions options() {
        return options;
    }

    public ServiceCaller<S> options(CallOptions options) {
        this.options = options;
        return this;
    }

    @Override
    public <REQ extends Request, RES extends Response> RES call(
        MethodDescriptor<REQ, RES> method, long requestId, REQ request, ServiceCallCycles<REQ, RES> handler
    ) {
        handler.before(request, options, requestId);
        Channel channel = channelProvider.channel();
        int retry = this.retry;
        boolean connected = false;
        Map<String, Integer> errMsgs = new HashMap<>();
        String methodName = method.getFullMethodName();
        REQ lastRequest = null;
        while (retry-- > 0) {
            try {
                if (channel == null) {
                    channel = updateChannel(channel, requestId);
                    continue;
                }
                channelProvider.before(request);
                RES response = handler.call(method, request, options, channel, requestId, handler);
                if (response == null) {
                    channel = updateChannel(channel, requestId);
                    continue;
                }
                connected = true;
                channelProvider.after(response);
                if (!response.isOk$()) {
                    Error error = response.getError();
                    int errCode = error.getErrcode().number();
                    errMsgs.compute(
                        channel.authority() + ">>" + error.getErrmsg(), (k, v) -> v == null ? 1 : v + 1
                    );
                    switch (handler.onErrStrategy(
                        errorToStrategy(errCode),
                        this.retry, retry, request, response, options, channel.authority(), requestId
                    )) {
                        case RETRY:
                            handler.onRetry(request, response, options, channel.authority(), requestId);
                            if (errCode == Errno.ERAFT_NOTLEADER.number) {
                                channel = updateChannel(channel, requestId);
                            } else {
                                waitRetry();
                            }
                            continue;
                        case FAILED:
                            handler.onFailed(request, response, options, channel.authority(), requestId);
                            throw new RequestErrorException(errCode, error.getErrmsg());
                        case REFRESH:
                            handler.onRefresh(request, response, options, channel.authority(), requestId);
                            channel = updateChannel(channel, requestId);
                            throw new InvalidRouteTableException(error.getErrmsg());
                        case IGNORE:
                            handler.onIgnore(request, response, options, channel.authority(), requestId);
                            return null;
                        default:
                            throw new IllegalStateException("Unexpected value: " + errorToStrategy(errCode));
                    }
                }
                handler.after(request, response, options, channel.authority(), requestId);
                return response;
            } catch (Exception e) {
                if (e instanceof RequestErrorException || e instanceof InvalidRouteTableException) {
                    throw e;
                }
                handler.onException(lastRequest, e, options, channel == null ? null : channel.authority(), requestId);
                errMsgs.compute(e.getMessage(), (k, v) -> v == null ? 1 : v + 1);
                channel = updateChannel(channel, requestId);
            }
        }

        throw generateException(methodName, requestId, lastRequest, connected, errMsgs, handler);
    }

    private void waitRetry() {
        LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(1));
    }

    private Channel updateChannel(Channel channel, long trace) {
        channelProvider.refresh(channel, trace);
        waitRetry();
        return channelProvider.channel();
    }

    private <REQ extends Request> RuntimeException generateException(
        String name, long traceId, REQ request, boolean connected, Map<String, Integer> errMsgs, ServiceCallCycles handler
    ) {
        // if connected is false, means can not get leader connection
        if (connected) {
            StringBuilder errMsgBuilder = new StringBuilder();
            errMsgBuilder.append("task: ").append(name).append(", trace: ").append(traceId).append(" ==>> ");
            errMsgs.forEach(
                (k, v) -> errMsgBuilder.append('[').append(v).append("] times [").append(k).append(']').append(", ")
            );
            ExhaustedRetryException exception = new ExhaustedRetryException(
                "Exec attempts exhausted, failed to exec " + name + ", " + errMsgBuilder
            );
            handler.onThrow(request, exception, options, traceId);
            throw exception;
        } else {
            handler.onNonConnection(request, options, traceId);
            throw new ExhaustedRetryException(
                "Exec [" + traceId + "] [" + name + "] error, " + "transform leader attempts exhausted."
            );
        }
    }

}
