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
