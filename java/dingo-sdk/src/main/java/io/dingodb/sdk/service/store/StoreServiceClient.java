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
import io.dingodb.common.Common;
import io.dingodb.error.ErrorOuterClass;
import io.dingodb.sdk.common.DingoClientException;
import io.dingodb.sdk.common.DingoCommonId;
import io.dingodb.sdk.common.KeyValue;
import io.dingodb.sdk.common.Location;
import io.dingodb.sdk.common.Range;
import io.dingodb.sdk.common.RangeWithOptions;
import io.dingodb.sdk.common.SDKCommonId;
import io.dingodb.sdk.common.table.RangeDistribution;
import io.dingodb.sdk.common.utils.EntityConversion;
import io.dingodb.sdk.service.connector.ServiceConnector;
import io.dingodb.sdk.service.connector.StoreServiceConnector;
import io.dingodb.sdk.service.meta.MetaServiceClient;
import io.dingodb.store.Store;
import io.dingodb.store.StoreServiceGrpc;
import lombok.extern.slf4j.Slf4j;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static io.dingodb.sdk.common.utils.EntityConversion.mapping;

@Slf4j
public class StoreServiceClient {

    private final Map<DingoCommonId, StoreServiceConnector> connectorCache = new ConcurrentHashMap<>();
    private final MetaServiceClient rootMetaService;

    private Integer retryTimes;

    public StoreServiceClient(MetaServiceClient rootMetaService) {
        this(rootMetaService, 20);
    }

    public StoreServiceClient(MetaServiceClient rootMetaService, Integer retryTimes) {
        this.rootMetaService = rootMetaService;
        this.retryTimes = retryTimes;
    }

    public Supplier<Location> locationSupplier(DingoCommonId schemaId, DingoCommonId tableId, DingoCommonId regionId) {
        return () -> rootMetaService.getSubMetaService(schemaId).getRangeDistribution(tableId).values().stream()
            .filter(rd -> rd.getId().equals(regionId))
            .findAny()
            .map(RangeDistribution::getLeader)
            .orElse(null);
    }

    public StoreServiceConnector getStoreConnector(DingoCommonId tableId, DingoCommonId regionId) {
        // Schema parent is root, table parent is schema, so use root schema id and table parent id create schema id.
        SDKCommonId schemaId = new SDKCommonId(
            DingoCommonId.Type.ENTITY_TYPE_SCHEMA, rootMetaService.id().getEntityId(), tableId.parentId()
        );
        return connectorCache.computeIfAbsent(
            regionId,
            __ -> new StoreServiceConnector(
                locationSupplier(schemaId, tableId, regionId),
                rootMetaService.getRangeDistribution(tableId).values().stream()
                    .filter(rd -> rd.getId().equals(regionId))
                    .findAny()
                    .map(RangeDistribution::getVoters)
                    .orElseThrow(() -> new RuntimeException("Cannot find region or region not have voters. "))
            )
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
            return new ServiceConnector.Response<>(res.getError(), res.getValue().toByteArray());
        }, retryTimes, tableId, regionId);
    }

    public List<KeyValue> kvBatchGet(DingoCommonId tableId, DingoCommonId regionId, List<byte[]> keys) {
        return exec(stub -> {
            Store.KvBatchGetRequest req = Store.KvBatchGetRequest.newBuilder()
                    .setRegionId(regionId.entityId())
                    .addAllKeys(keys.stream().map(ByteString::copyFrom).collect(Collectors.toList()))
                    .build();
            Store.KvBatchGetResponse res = stub.kvBatchGet(req);
            return new ServiceConnector.Response<>(
                res.getError(),
                res.getKvsList().stream().map(EntityConversion::mapping).collect(Collectors.toList())
            );
        }, retryTimes, tableId, regionId);
    }

    public Iterator<KeyValue> scan(
        DingoCommonId tableId, DingoCommonId regionId, Range range, boolean withStart, boolean withEnd
    ) {
        return new ScanIterator(getStoreConnector(tableId, regionId).getStub(),
               regionId.entityId(),
               Common.RangeWithOptions.newBuilder()
               .setRange(
                   Common.Range.newBuilder()
                       .setStartKey(ByteString.copyFrom(range.getStartKey()))
                       .setEndKey(ByteString.copyFrom(range.getEndKey()))
                       .build())
               .setWithStart(withStart)
               .setWithEnd(withEnd)
               .build(),
               false);
    }

    public boolean kvPut(DingoCommonId tableId, DingoCommonId regionId, KeyValue keyValue) {
        return exec(stub -> {
            Store.KvPutRequest req = Store.KvPutRequest.newBuilder()
                .setRegionId(regionId.entityId())
                .setKv(mapping(keyValue))
                .build();
            Store.KvPutResponse res = stub.kvPut(req);
            return new ServiceConnector.Response<>(res.getError(), true);
        }, retryTimes, tableId, regionId);
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
            return new ServiceConnector.Response<>(res.getError(), true);
        }, retryTimes, tableId, regionId);
    }

    public boolean kvPutIfAbsent(DingoCommonId tableId, DingoCommonId regionId, KeyValue keyValue) {
        return exec(stub -> {
            Store.KvPutIfAbsentRequest req = Store.KvPutIfAbsentRequest.newBuilder()
                    .setRegionId(regionId.entityId())
                    .setKv(mapping(keyValue))
                    .build();
            Store.KvPutIfAbsentResponse res = stub.kvPutIfAbsent(req);
            return new ServiceConnector.Response<>(res.getError(), res.getKeyState());
        }, retryTimes, tableId, regionId);
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
            return new ServiceConnector.Response<>(res.getError(), res.getKeyStatesList());
        }, retryTimes, tableId, regionId);
    }

    public List<Boolean> kvBatchDelete(DingoCommonId tableId, DingoCommonId regionId, List<byte[]> keys) {
        return exec(stub -> {
            Store.KvBatchDeleteRequest req = Store.KvBatchDeleteRequest.newBuilder()
                    .setRegionId(regionId.entityId())
                    .addAllKeys(keys.stream().map(ByteString::copyFrom).collect(Collectors.toList()))
                    .build();
            Store.KvBatchDeleteResponse res = stub.kvBatchDelete(req);
            return new ServiceConnector.Response<>(res.getError(), res.getKeyStatesList());
        }, retryTimes, tableId, regionId);
    }

    public long kvDeleteRange(DingoCommonId tableId, DingoCommonId regionId, RangeWithOptions options) {
        return exec(stub -> {
            Store.KvDeleteRangeRequest req = Store.KvDeleteRangeRequest.newBuilder()
                    .setRegionId(regionId.entityId())
                    .setRange(mapping(options))
                    .build();
            Store.KvDeleteRangeResponse res = stub.kvDeleteRange(req);
            return new ServiceConnector.Response<>(res.getError(), res.getDeleteCount());
        }, retryTimes, tableId, regionId);
    }

    private <R> R exec(
            Function<StoreServiceGrpc.StoreServiceBlockingStub, ServiceConnector.Response<R>> function,
            int retryTimes,
            DingoCommonId tableId,
            DingoCommonId regionId
    ) {
        return getStoreConnector(tableId, regionId).exec(function, retryTimes, err -> true).getResponse();

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
