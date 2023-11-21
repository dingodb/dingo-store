package io.dingodb.sdk.service.rpc;

import io.grpc.ClientCall;
import io.grpc.Metadata;
import io.grpc.Status;

import java.util.concurrent.CompletableFuture;

public class RpcFuture<T> extends CompletableFuture<T> {

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

    protected final Listener listener = new Listener();

    private Metadata headers;

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
        complete(message);
    }

    public void onClose(Status status, Metadata trailers) {
        this.status = status;
        this.trailers = trailers;
    }

    public void onReady() {
        // skip
    }
}
