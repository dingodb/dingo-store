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

package io.dingodb.client.operation;

import com.google.protobuf.ByteString;
import io.dingodb.client.ContextForStore;
import io.dingodb.client.IStoreOperation;
import io.dingodb.client.ResultForStore;
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
