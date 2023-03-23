/*
 * Copyright 2021 DataCanvas
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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