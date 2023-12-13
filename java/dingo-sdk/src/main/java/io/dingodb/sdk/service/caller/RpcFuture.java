package io.dingodb.sdk.service.caller;

import io.dingodb.sdk.common.DingoClientException;
import io.grpc.ClientCall;
import io.grpc.Metadata;
import io.grpc.Status;
import lombok.experimental.Delegate;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

public class RpcFuture<T> implements CompletionStage<T> {

    public class Listener extends ClientCall.Listener<T> {

        @Override
        public void onHeaders(Metadata headers) {
            RpcFuture.this.onHeaders(headers);
        }

        @Override
        public void onMessage(T message) {
            RpcFuture.this.onMessage(message);
        }

        @Override
        public void onClose(Status status, Metadata trailers) {
            RpcFuture.this.onClose(status, trailers);
        }

        @Override
        public void onReady() {
            RpcFuture.this.onReady();
        }
    }

    @Delegate
    protected final CompletableFuture<T> future = new CompletableFuture<>();
    protected final Listener listener = new Listener();

    private Metadata headers;
    private T message;

    private Status status;
    private Metadata trailers;

    public RpcFuture() {
        super();
    }

    public Metadata headers() {
        return headers;
    }

    public Status status() {
        return status;
    }

    public Metadata trailers() {
        return trailers;
    }

    public void onHeaders(Metadata headers) {
        this.headers = headers;
    }

    public void onMessage(T message) {
        this.message = message;
    }

    public void onClose(Status status, Metadata trailers) {
        this.status = status;
        this.trailers = trailers;
        if (!status.isOk()) {
            if (status.getCause() != null) {
                completeExceptionally(status.getCause());
            } else {
                completeExceptionally(new DingoClientException(status.getCode().value(), status.getDescription()));
            }
        }
        complete(message);
    }

    public void onReady() {
        // skip
    }
}
