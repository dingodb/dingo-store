/*
 * Copyright 2021, Zetyun DataPortal All rights reserved.
 */

package io.dingodb.client;

import com.google.protobuf.ByteString;
import io.dingodb.sdk.service.store.StoreServiceClient;
import io.dingodb.error.ErrorOuterClass;
import io.dingodb.store.Store;

import java.util.List;
import java.util.stream.Collectors;

public class GetOperation implements IStoreOperation {

    private static final GetOperation INSTANCE = new GetOperation();

    private GetOperation() {

    }

    public static GetOperation getInstance() {
        return INSTANCE;
    }

    @Override
    public ResultForStore doOperation(StoreServiceClient storeServiceClient, ContextForStore contextForStore) {
        List<ByteString> bytes = contextForStore.getStartKeyInBytes().stream()
            .map(ByteString::copyFrom)
            .collect(Collectors.toList());

        Store.KvBatchGetRequest request = Store.KvBatchGetRequest.newBuilder()
            .setRegionId(contextForStore.getRegionId().getEntityId())
            .addAllKeys(bytes)
            .build();

        Store.KvBatchGetResponse response = storeServiceClient.kvBatchGet(request);
        ErrorOuterClass.Error error = response.getError();

        return new ResultForStore(error.getErrcodeValue(), error.getErrmsg(), response.getKvsList());
    }
}
