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

import com.google.protobuf.ByteString;
import io.dingodb.error.ErrorOuterClass;
import io.dingodb.sdk.common.DingoClientException;
import io.dingodb.sdk.common.DingoCommonId;
import io.dingodb.sdk.common.KeyValue;
import io.dingodb.sdk.common.Range;
import io.dingodb.sdk.common.table.RangeDistribution;
import io.dingodb.sdk.common.utils.EntityConversion;
import io.dingodb.sdk.service.connector.ServiceConnector;
import io.dingodb.sdk.service.connector.StoreConnector;
import io.dingodb.sdk.service.meta.MetaClient;
import io.dingodb.store.Store;
import io.dingodb.store.StoreServiceGrpc;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;

import static io.dingodb.sdk.common.utils.EntityConversion.mapping;

@Slf4j
public class StoreServiceClient {

    private Map<DingoCommonId, StoreConnector> connectorCache = new ConcurrentHashMap<>();
    private Map<DingoCommonId, RangeDistribution> rangeDistributionCache = new ConcurrentHashMap<>();

    private MetaClient metaClient;

    public StoreServiceClient(MetaClient metaClient) {
        this.metaClient = metaClient;
    }

    public StoreConnector getStoreConnector(DingoCommonId tableId, DingoCommonId regionId) {
        rangeDistributionCache.computeIfAbsent(regionId,
                __ -> metaClient.getRangeDistribution(tableId).values().stream()
                        .filter(range -> range.getId().equals(regionId))
                        .findAny().orElse(null)
        );
        RangeDistribution rangeDistribution = rangeDistributionCache.get(regionId);
        return connectorCache.computeIfAbsent(regionId, __ ->
                new StoreConnector(metaClient, tableId, rangeDistribution.getLeader(), rangeDistribution.getVoters())
        );
    }

    public void shutdown() {
        connectorCache.values().forEach(ServiceConnector::shutdown);
    }

    public byte[] kvGet(DingoCommonId tableId, DingoCommonId regionId, byte[] key) {
        return exec(stub -> {
            Store.KvGetRequest req = Store.KvGetRequest.newBuilder()
                    .setRegionId(regionId.entityId())
                    .setKey(ByteString.copyFrom(key))
                    .build();
            Store.KvGetResponse res = stub.kvGet(req);
            check(res.getError());
            return res.getValue().toByteArray();
        }, 10, tableId, regionId);
    }

    public List<KeyValue> kvBatchGet(DingoCommonId tableId, DingoCommonId regionId, List<byte[]> keys) {
        return exec(stub -> {
            Store.KvBatchGetRequest req = Store.KvBatchGetRequest.newBuilder()
                    .setRegionId(regionId.entityId())
                    .addAllKeys(keys.stream().map(ByteString::copyFrom).collect(Collectors.toList()))
                    .build();
            if (stub == null) {
                throw new DingoClientException(-1, "blockingStub is null");
            }
            Store.KvBatchGetResponse res = stub.kvBatchGet(req);
            check(res.getError());
            return res.getKvsList().stream().map(EntityConversion::mapping).collect(Collectors.toList());
        }, 10, tableId, regionId);
    }

    public boolean kvPut(DingoCommonId tableId, DingoCommonId regionId, KeyValue keyValue) {
        return exec(stub -> {
            Store.KvPutRequest req = Store.KvPutRequest.newBuilder()
                .setRegionId(regionId.entityId())
                .setKv(mapping(keyValue))
                .build();
            Store.KvPutResponse res = stub.kvPut(req);
            check(res.getError());
            return true;
        }, 10, tableId, regionId);
    }

    public boolean kvBatchPut(DingoCommonId tableId, DingoCommonId regionId, List<KeyValue> keyValues) {
        return exec(stub -> {
            Store.KvBatchPutRequest req = Store.KvBatchPutRequest.newBuilder()
                .setRegionId(regionId.entityId())
                .addAllKvs(keyValues.stream().map(EntityConversion::mapping).collect(Collectors.toList()))
                .build();
            if (stub == null) {
                throw new DingoClientException(-1, "blockingStub is null");
            }
            Store.KvBatchPutResponse res = stub.kvBatchPut(req);
            check(res.getError());
            return true;
        }, 10, tableId, regionId);
    }

    public boolean kvPutIfAbsent(DingoCommonId tableId, DingoCommonId regionId, KeyValue keyValue) {
        return exec(stub -> {
            Store.KvPutIfAbsentRequest req = Store.KvPutIfAbsentRequest.newBuilder()
                    .setRegionId(regionId.entityId())
                    .setKv(mapping(keyValue))
                    .build();
            Store.KvPutIfAbsentResponse res = stub.kvPutIfAbsent(req);
            check(res.getError());
            return res.getKeyState();
        }, 10, tableId, regionId);
    }

    public List<Boolean> kvBatchPutIfAbsent(DingoCommonId tableId, DingoCommonId regionId, List<KeyValue> keyValues) {
        return kvBatchPutIfAbsent(tableId, regionId, keyValues, false);
    }

    public List<Boolean> kvBatchPutIfAbsent(DingoCommonId tableId, DingoCommonId regionId, List<KeyValue> keyValues, boolean isAtomic) {
        return exec(stub -> {
            Store.KvBatchPutIfAbsentRequest req = Store.KvBatchPutIfAbsentRequest.newBuilder()
                    .setRegionId(regionId.entityId())
                    .addAllKvs(keyValues.stream().map(EntityConversion::mapping).collect(Collectors.toList()))
                    .setIsAtomic(isAtomic)
                    .build();
            Store.KvBatchPutIfAbsentResponse res = stub.kvBatchPutIfAbsent(req);
            check(res.getError());
            return res.getKeyStatesList();
        }, 10, tableId, regionId);
    }

    public boolean kvBatchDelete(DingoCommonId tableId, DingoCommonId regionId, List<byte[]> keys) {
        return exec(stub -> {
            Store.KvBatchDeleteRequest req = Store.KvBatchDeleteRequest.newBuilder()
                    .setRegionId(regionId.entityId())
                    .addAllKeys(keys.stream().map(ByteString::copyFrom).collect(Collectors.toList()))
                    .build();
            Store.KvBatchDeleteResponse res = stub.kvBatchDelete(req);
            check(res.getError());
            return true;
        }, 10, tableId, regionId);
    }

    public boolean kvDeleteRange(DingoCommonId tableId, DingoCommonId regionId, Range range) {
        return exec(stub -> {
            /*Store.KvDeleteRangeRequest req = Store.KvDeleteRangeRequest.newBuilder()
                    .setRegionId(regionId.entityId())
                    .setRange(mapping(range))
                    .build();
            Store.KvDeleteRangeResponse res = stub.kvDeleteRange(req);
            check(res.getError());*/
            return true;
        }, 10, tableId, regionId);
    }

    private <R> R exec(
            Function<StoreServiceGrpc.StoreServiceBlockingStub, R> function,
            int retryTimes,
            DingoCommonId tableId,
            DingoCommonId regionId
    ) {
        try {
            return function.apply(getStoreConnector(tableId, regionId).getBlockingStub());
        } catch (DingoClientException ex) {
            if (ex.getErrorCode() != 0 && retryTimes > 0) {
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                rangeDistributionCache.remove(regionId);
                connectorCache.remove(regionId);
                System.out.println("---- retry is :" + retryTimes);
                return exec(function, retryTimes - 1, tableId, regionId);
            }
            throw ex;
        } catch (Exception e) {
            throw new RuntimeException("Retry attempts exhausted, failed to execute operation, e: " + e);
        }
    }

    private void check(ErrorOuterClass.Error error) {
        if (error == null) {
            return;
        }
        if (error.getErrcodeValue() != 0) {
            throw new DingoClientException(error.getErrcodeValue(), error.getErrmsg());
        }
    }
}
