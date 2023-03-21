/*
 * Copyright 2021, Zetyun DataPortal All rights reserved.
 */

package io.dingodb.client.operation;

import com.google.protobuf.ByteString;
import io.dingodb.client.ContextForStore;
import io.dingodb.client.IStoreOperation;
import io.dingodb.client.ResultForStore;
import io.dingodb.error.ErrorOuterClass;
import io.dingodb.sdk.service.store.StoreServiceClient;
import io.dingodb.store.Store;

import java.util.List;
import java.util.stream.Collectors;

public class DeleteOperation implements IStoreOperation {

    private static final DeleteOperation INSTANCE = new DeleteOperation();

    private DeleteOperation() {

    }

    public static DeleteOperation getInstance() {
        return INSTANCE;
    }

    @Override
    public ResultForStore doOperation(StoreServiceClient storeServiceClient, ContextForStore contextForStore) {
        List<ByteString> bytes = contextForStore.getStartKeyInBytes().stream()
                .map(ByteString::copyFrom)
                .collect(Collectors.toList());

        Store.KvBatchDeleteRequest request = Store.KvBatchDeleteRequest.newBuilder()
                .setRegionId(contextForStore.getRegionId().getEntityId())
                .addAllKeys(bytes)
                .build();

        Store.KvBatchDeleteResponse response = storeServiceClient.kvBatchDelete(request);
        ErrorOuterClass.Error error = response.getError();

        return new ResultForStore(error.getErrcodeValue(), error.getErrmsg());
    }
}
