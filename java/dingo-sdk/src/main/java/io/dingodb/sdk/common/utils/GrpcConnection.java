/*
 * Copyright 2021, Zetyun DataPortal All rights reserved.
 */

package io.dingodb.sdk.common.utils;

import io.dingodb.coordinator.CoordinatorServiceGrpc;
import io.dingodb.meta.MetaServiceGrpc;
import io.dingodb.store.StoreServiceGrpc;
import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import io.grpc.ManagedChannel;
import io.grpc.stub.AbstractBlockingStub;
import lombok.NonNull;

import javax.activation.UnsupportedDataTypeException;

public class GrpcConnection {

    public static ManagedChannel newChannel(@NonNull String target) {
        return Grpc.newChannelBuilder(target, InsecureChannelCredentials.create()).build();
    }

    public static AbstractBlockingStub<?> newBlockingStub(@NonNull ManagedChannel channel, String ident) throws UnsupportedDataTypeException {
        switch (ident.toUpperCase()) {
            case "STORE":
                return StoreServiceGrpc.newBlockingStub(channel);
            case "META":
                return MetaServiceGrpc.newBlockingStub(channel);
            case "COORDINATOR":
                return CoordinatorServiceGrpc.newBlockingStub(channel);
            default:
                throw new UnsupportedDataTypeException(ident);
        }
    }
}