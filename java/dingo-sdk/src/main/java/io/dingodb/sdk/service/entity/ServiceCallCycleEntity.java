package io.dingodb.sdk.service.entity;

import io.grpc.CallOptions;
import lombok.AllArgsConstructor;
import lombok.Builder;

@Builder
@AllArgsConstructor
public class ServiceCallCycleEntity {

    public final String step;
    @Builder.Default
    public final String status = "OK";
    public final String remote;
    public final String method;
    @Builder.Default
    public final long timestamp = System.currentTimeMillis();
    public final long trace;
    public final Message.Request request;
    public final Message.Response response;
    public final CallOptions options;
    public final Exception exception;

}
