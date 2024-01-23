package io.dingodb.sdk.common.utils;

import lombok.AllArgsConstructor;
import lombok.experimental.Delegate;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

@AllArgsConstructor
public class Future implements java.util.concurrent.Future<Void>, CompletionStage<Void> {

    interface IFuture extends java.util.concurrent.Future<Void> {
    }

    interface ICompletionStage extends CompletionStage<Void> {
    }

    @Delegate(types = {IFuture.class, ICompletionStage.class})
    private final CompletableFuture<Void> delegate;

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<Void> toCompletableFuture() {
        throw new UnsupportedOperationException();
    }
}
