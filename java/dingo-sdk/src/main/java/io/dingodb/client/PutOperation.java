/*
 * Copyright 2021, Zetyun DataPortal All rights reserved.
 */

package io.dingodb.client;

import com.google.protobuf.ByteString;
import io.dingodb.common.Common;
import io.dingodb.sdk.service.store.StoreServiceClient;
import io.dingodb.error.ErrorOuterClass;
import io.dingodb.store.Store;

import java.util.stream.Collectors;

public class PutOperation implements IStoreOperation {

    private static final PutOperation INSTANCE = new PutOperation();

    private PutOperation() {
    }

    public static PutOperation getInstance() {
        return INSTANCE;
    }

    @Override
    public ResultForStore doOperation(StoreServiceClient storeServiceClient, ContextForStore contextForStore) {
        Store.KvBatchPutRequest request = Store.KvBatchPutRequest.newBuilder()
                .setRegionId(contextForStore.getRegionId().getEntityId())
                .addAllKvs(contextForStore.getRecordList().stream()
                    .map(kv ->
                        Common.KeyValue.newBuilder()
                            .setKey(ByteString.copyFrom(kv.getKey()))
                            .setValue(ByteString.copyFrom(kv.getValue()))
                            .build())
                    .collect(Collectors.toList()))
                .build();

        Store.KvBatchPutResponse response = storeServiceClient.kvBatchPut(request);
        ErrorOuterClass.Error error = response.getError();

        return new ResultForStore(error.getErrcodeValue(), error.getErrmsg());
    }
}
