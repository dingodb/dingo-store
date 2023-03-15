/*
 * Copyright 2021, Zetyun DataPortal All rights reserved.
 */

package io.dingodb.sdk.service.store;

import io.dingodb.sdk.common.utils.GrpcConnection;
import io.dingodb.store.Store;
import io.dingodb.store.StoreServiceGrpc;
import io.grpc.ManagedChannel;

import javax.activation.UnsupportedDataTypeException;

public class StoreServiceClient {

    private ManagedChannel channel;
    private StoreServiceGrpc.StoreServiceBlockingStub blockingStub;

    public StoreServiceClient(String target) {
        channel = GrpcConnection.newChannel(target);
        try {
            blockingStub =
                    (StoreServiceGrpc.StoreServiceBlockingStub) GrpcConnection.newBlockingStub(channel, "STORE");
        } catch (UnsupportedDataTypeException e) {
            throw new RuntimeException(e);
        }
    }

    public void shutdown() {
        channel.shutdown();
    }

    public Store.KvPutResponse kvPut(Store.KvPutRequest request) {
        return blockingStub.kvPut(request);
    }

    public Store.KvBatchPutResponse kvBatchPut(Store.KvBatchPutRequest request) {
        return blockingStub.kvBatchPut(request);
    }

    public Store.KvPutIfAbsentResponse kvPutIfAbsent(Store.KvPutIfAbsentRequest request) {
        return blockingStub.kvPutIfAbsent(request);
    }

    public Store.KvBatchPutIfAbsentResponse kvBatchPutIfAbsent(Store.KvBatchPutIfAbsentRequest request) {
        return blockingStub.kvBatchPutIfAbsent(request);
    }

    public Store.KvGetResponse kvGet(Store.KvGetRequest request) {
        return blockingStub.kvGet(request);
    }

    public Store.KvBatchGetResponse kvBatchGet(Store.KvBatchGetRequest request) {
        return blockingStub.kvBatchGet(request);
    }
}
