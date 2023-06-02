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
import io.dingodb.sdk.common.DingoClientException;
import io.dingodb.sdk.common.DingoCommonId;
import io.dingodb.sdk.common.KeyValue;
import io.dingodb.sdk.common.KeyValueWithExpect;
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

import java.util.ArrayList;
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

    private Supplier<Location> locationSupplier(DingoCommonId schemaId, DingoCommonId tableId, DingoCommonId regionId) {
        return () -> rootMetaService.getSubMetaService(schemaId).getRangeDistribution(tableId).values().stream()
            .filter(rd -> rd.getId().equals(regionId))
            .findAny()
            .map(RangeDistribution::getLeader)
            .orElse(null);
    }

    /**
     * Get store connector for the region of a specified table.
     * @param tableId table id
     * @param regionId region id
     * @return store connector
     */
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
        connectorCache.clear();
    }

    /**
     * Returns the value to which the specified key, or empty byte array if not have the key.
     * @param tableId table id of key
     * @param regionId region id of key
     * @param key key
     * @return value
     */
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

    /**
     * Returns the KeyValue list for the specified keys that are found.
     * @param tableId table id of keys
     * @param regionId region id of keys
     * @param keys keys, the keys must in same region
     * @return values
     */
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

    /**
     * Returns KeyValue iterator of scan, the scan is starting from the given start key,
     * traverse in order until a key is encountered that exceeds the specified end key.
     * Since the key is a variable-length byte array, the start and end specified in the range will
     * be treated as prefixes.
     * @param tableId table id
     * @param regionId region id
     * @param range key range, start and end must in same region
     * @param withStart is with start
     * @param withEnd is with end
     * @return KeyValue iterator of scan
     */
    public Iterator<KeyValue> scan(
        DingoCommonId tableId, DingoCommonId regionId, Range range, boolean withStart, boolean withEnd
    ) {
        return scan(tableId, regionId, range, withStart, withEnd, null);
    }

    /**
     * Returns KeyValue iterator of scan, the scan is starting from the given start key,
     * traverse in order until a key is encountered that exceeds the specified end key.
     * Since the key is a variable-length byte array, the start and end specified in the range will
     * be treated as prefixes.
     * @param tableId table id
     * @param regionId region id
     * @param range key range, start and end must in same region
     * @param withStart is with start
     * @param withEnd is with end
     * @param coprocessor coprocessor
     * @return KeyValue iterator of scan
     */
    public Iterator<KeyValue> scan(
        DingoCommonId tableId,
        DingoCommonId regionId,
        Range range,
        boolean withStart,
        boolean withEnd,
        Coprocessor coprocessor
    ) {
        return new ScanIterator(getStoreConnector(tableId, regionId),
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
                false,
                retryTimes,
                coprocessor
        );
    }

    /**
     * Put key and value to store.
     * @param tableId table id
     * @param regionId region id
     * @param keyValue key and value
     * @return is success
     */
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

    /**
     * Put key and value to store if the key is not in store.
     * @param tableId table id
     * @param regionId region id
     * @param keyValue key and value
     * @return true if key is not in store or false if the key exist in store
     */
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

    /**
     * Delete keys on store.
     * @param tableId table id
     * @param regionId region id
     * @param keys delete key list, must in same region
     * @return delete success or fail with keys
     */
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

    /**
     * Delete keys in range, range strategy like {@link StoreServiceClient#scan}.
     * @param tableId table id
     * @param regionId region id
     * @param range key range, start and end must in same region
     * @return delete keys count
     */
    public long kvDeleteRange(DingoCommonId tableId, DingoCommonId regionId, RangeWithOptions range) {
        return exec(stub -> {
            Store.KvDeleteRangeRequest req = Store.KvDeleteRangeRequest.newBuilder()
                    .setRegionId(regionId.entityId())
                    .setRange(mapping(range))
                    .build();
            Store.KvDeleteRangeResponse res = stub.kvDeleteRange(req);
            return new ServiceConnector.Response<>(res.getError(), res.getDeleteCount());
        }, retryTimes, tableId, regionId);
    }

    public boolean kvCompareAndSet(DingoCommonId tableId, DingoCommonId regionId, KeyValueWithExpect keyValue) {
        return exec(stub -> {
            Store.KvCompareAndSetRequest req = Store.KvCompareAndSetRequest.newBuilder()
                .setRegionId(regionId.entityId())
                .setKv(EntityConversion.mapping(keyValue))
                .setExpectValue(ByteString.copyFrom(keyValue.expect))
                .build();
            Store.KvCompareAndSetResponse res = stub.kvCompareAndSet(req);
            return new ServiceConnector.Response<>(res.getError(), res.getKeyState());
        }, retryTimes, tableId, regionId);
    }

    public List<Boolean> kvBatchCompareAndSet(
        DingoCommonId tableId, DingoCommonId regionId, List<KeyValueWithExpect> keyValues, boolean isAtomic
    ) {
        List<Common.KeyValue> kvs = new ArrayList<>();
        List<ByteString> expects = new ArrayList<>();
        keyValues.stream().peek(__ -> kvs.add(mapping(__))).forEach(__ -> expects.add(ByteString.copyFrom(__.expect)));
        return exec(stub -> {
            Store.KvBatchCompareAndSetRequest req = Store.KvBatchCompareAndSetRequest.newBuilder()
                .setRegionId(regionId.entityId())
                .addAllKvs(kvs)
                .addAllExpectValues(expects)
                .setIsAtomic(isAtomic)
                .build();
            Store.KvBatchCompareAndSetResponse res = stub.kvBatchCompareAndSet(req);
            return new ServiceConnector.Response<>(res.getError(), res.getKeyStatesList());
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
}
