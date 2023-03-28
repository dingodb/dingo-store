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

    public Store.KvBatchDeleteResponse kvBatchDelete(Store.KvBatchDeleteRequest request) {
        return blockingStub.kvBatchDelete(request);
    }
}
