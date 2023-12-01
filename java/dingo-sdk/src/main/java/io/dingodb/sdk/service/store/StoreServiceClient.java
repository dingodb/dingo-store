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
import io.dingodb.meta.Meta;
import io.dingodb.sdk.common.Context;
import io.dingodb.sdk.common.DingoCommonId;
import io.dingodb.sdk.common.KeyValue;
import io.dingodb.sdk.common.KeyValueWithExpect;
import io.dingodb.sdk.common.Location;
import io.dingodb.sdk.common.Range;
import io.dingodb.sdk.common.RangeWithOptions;
import io.dingodb.sdk.common.SDKCommonId;
import io.dingodb.sdk.common.table.RangeDistribution;
import io.dingodb.sdk.common.utils.EntityConversion;
import io.dingodb.sdk.common.utils.ErrorCodeUtils;
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
import static io.dingodb.sdk.common.utils.StackTraces.CURRENT_STACK;
import static io.dingodb.sdk.common.utils.StackTraces.stack;

@Slf4j
public class StoreServiceClient {

    private final Map<DingoCommonId, StoreServiceConnector> connectorCache = new ConcurrentHashMap<>();
    private final Map<DingoCommonId, Context> contextCache = new ConcurrentHashMap<>();
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
        if (tableId.type() == DingoCommonId.Type.ENTITY_TYPE_TABLE) {
            return () -> rootMetaService.getSubMetaService(schemaId).getRangeDistribution(tableId).values().stream()
                    .filter(rd -> rd.getId().equals(regionId))
                    .findAny()
                    .map(this::cacheRangeEpoch)
                    .map(RangeDistribution::getLeader)
                    .orElse(null);
        }
        if (tableId.type() == DingoCommonId.Type.ENTITY_TYPE_INDEX) {
            return () -> rootMetaService.getSubMetaService(schemaId).getIndexRangeDistribution(tableId).values().stream()
                    .filter(rd -> rd.getId().equals(regionId))
                    .findAny()
                    .map(this::cacheRangeEpoch)
                    .map(RangeDistribution::getLeader)
                    .orElse(null);
        }
        return null;
    }

    private RangeDistribution cacheRangeEpoch(RangeDistribution rangeDistribution) {
        Context context = contextCache.get(rangeDistribution.getId());
        if (context != null && !context.getRegionEpoch().equals(rangeDistribution.getRegionEpoch())) {
            context.setRegionEpoch(rangeDistribution.getRegionEpoch());
        } else {
            contextCache.put(rangeDistribution.getId(), new Context(rangeDistribution.getId(), rangeDistribution.getRegionEpoch()));
        }
        return rangeDistribution;
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
            regionId, __ -> new StoreServiceConnector(locationSupplier(schemaId, tableId, regionId))
        );
    }


    public void shutdown() {
        connectorCache.clear();
        contextCache.clear();
    }

    /**
     * Returns the value to which the specified key, or empty byte array if not have the key.
     * @param tableId table id of key
     * @param regionId region id of key
     * @param key key
     * @return value
     */
    public byte[] kvGet(DingoCommonId tableId, DingoCommonId regionId, byte[] key) {
        Store.KvGetRequest.Builder builder = Store.KvGetRequest.newBuilder().setKey(ByteString.copyFrom(key));
        return exec(stub -> stub.kvGet(builder.setContext(mapping(contextCache.get(regionId))).build()),
                retryTimes, tableId, regionId).getValue().toByteArray();
    }

    /**
     * Returns the KeyValue list for the specified keys that are found.
     * @param tableId table id of keys
     * @param regionId region id of keys
     * @param keys keys, the keys must in same region
     * @return values
     */
    public List<KeyValue> kvBatchGet(DingoCommonId tableId, DingoCommonId regionId, List<byte[]> keys) {
        Store.KvBatchGetRequest.Builder builder = Store.KvBatchGetRequest.newBuilder()
                .addAllKeys(keys.stream().map(ByteString::copyFrom).collect(Collectors.toList()));
        return exec(stub -> stub.kvBatchGet(builder.setContext(mapping(contextCache.get(regionId))).build()),
                retryTimes, tableId, regionId).getKvsList().stream()
                .map(EntityConversion::mapping)
                .collect(Collectors.toList());
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
                () -> contextCache.get(regionId),
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
        Store.KvPutRequest.Builder builder = Store.KvPutRequest.newBuilder().setKv(mapping(keyValue));
        exec(stub -> stub.kvPut(builder.setContext(mapping(contextCache.get(regionId))).build()),
                retryTimes, tableId, regionId);
        return true;
    }

    public boolean kvBatchPut(DingoCommonId tableId, DingoCommonId regionId, List<KeyValue> keyValues) {
        Store.KvBatchPutRequest.Builder builder = Store.KvBatchPutRequest.newBuilder()
                .addAllKvs(keyValues.stream().map(EntityConversion::mapping).collect(Collectors.toList()));
        exec(stub -> stub.kvBatchPut(builder.setContext(mapping(contextCache.get(regionId))).build()),
                retryTimes, tableId, regionId);
        return true;
    }

    /**
     * Put key and value to store if the key is not in store.
     * @param tableId table id
     * @param regionId region id
     * @param keyValue key and value
     * @return true if key is not in store or false if the key exist in store
     */
    public boolean kvPutIfAbsent(DingoCommonId tableId, DingoCommonId regionId, KeyValue keyValue) {
        Store.KvPutIfAbsentRequest.Builder builder = Store.KvPutIfAbsentRequest.newBuilder().setKv(mapping(keyValue));
        return exec(stub -> stub.kvPutIfAbsent(builder.setContext(mapping(contextCache.get(regionId))).build()),
                retryTimes, tableId, regionId).getKeyState();
    }

    public List<Boolean> kvBatchPutIfAbsent(DingoCommonId tableId, DingoCommonId regionId, List<KeyValue> keyValues) {
        return kvBatchPutIfAbsent(tableId, regionId, keyValues, false);
    }

    public List<Boolean> kvBatchPutIfAbsent(
            DingoCommonId tableId,
            DingoCommonId regionId,
            List<KeyValue> keyValues,
            boolean isAtomic) {
        Store.KvBatchPutIfAbsentRequest.Builder builder = Store.KvBatchPutIfAbsentRequest.newBuilder()
                .addAllKvs(keyValues.stream().map(EntityConversion::mapping).collect(Collectors.toList()))
                .setIsAtomic(isAtomic);
        return exec(stub -> stub.kvBatchPutIfAbsent(builder.setContext(mapping(contextCache.get(regionId))).build()),
                retryTimes, tableId, regionId).getKeyStatesList();
    }

    /**
     * Delete keys on store.
     * @param tableId table id
     * @param regionId region id
     * @param keys delete key list, must in same region
     * @return delete success or fail with keys
     */
    public List<Boolean> kvBatchDelete(DingoCommonId tableId, DingoCommonId regionId, List<byte[]> keys) {
        Store.KvBatchDeleteRequest.Builder builder = Store.KvBatchDeleteRequest.newBuilder()
                .addAllKeys(keys.stream().map(ByteString::copyFrom).collect(Collectors.toList()));
        return exec(stub -> stub.kvBatchDelete(builder.setContext(mapping(contextCache.get(regionId))).build()),
                retryTimes, tableId, regionId).getKeyStatesList();
    }

    /**
     * Delete keys in range, range strategy like {@link StoreServiceClient#scan}.
     * @param tableId table id
     * @param regionId region id
     * @param range key range, start and end must in same region
     * @return delete keys count
     */
    public long kvDeleteRange(DingoCommonId tableId, DingoCommonId regionId, RangeWithOptions range) {
        Store.KvDeleteRangeRequest.Builder builder = Store.KvDeleteRangeRequest.newBuilder().setRange(mapping(range));
        return exec(stub -> stub.kvDeleteRange(builder.setContext(mapping(contextCache.get(regionId))).build()),
                retryTimes, tableId, regionId).getDeleteCount();
    }

    public boolean kvCompareAndSet(DingoCommonId tableId, DingoCommonId regionId, KeyValueWithExpect keyValue) {
        Store.KvCompareAndSetRequest.Builder builder = Store.KvCompareAndSetRequest.newBuilder()
                .setKv(mapping(keyValue))
                .setExpectValue(ByteString.copyFrom(keyValue.expect));
        return exec(stub -> stub.kvCompareAndSet(builder.setContext(mapping(contextCache.get(regionId))).build()),
                retryTimes, tableId, regionId).getKeyState();
    }

    public List<Boolean> kvBatchCompareAndSet(
        DingoCommonId tableId, DingoCommonId regionId, List<KeyValueWithExpect> keyValues, boolean isAtomic
    ) {
        List<Common.KeyValue> kvs = new ArrayList<>();
        List<ByteString> expects = new ArrayList<>();
        keyValues.stream().peek(__ -> kvs.add(mapping(__))).forEach(__ -> expects.add(ByteString.copyFrom(__.expect)));
        Store.KvBatchCompareAndSetRequest.Builder builder = Store.KvBatchCompareAndSetRequest.newBuilder()
                .addAllKvs(kvs)
                .addAllExpectValues(expects)
                .setIsAtomic(isAtomic);
        return exec(stub -> stub.kvBatchCompareAndSet(builder.setContext(mapping(contextCache.get(regionId))).build()),
                retryTimes, tableId, regionId).getKeyStatesList();
    }

    private <R> R exec(
            Function<StoreServiceGrpc.StoreServiceBlockingStub, R> function,
            int retryTimes,
            DingoCommonId tableId,
            DingoCommonId regionId
    ) {
        String stack = stack(CURRENT_STACK + 1);
        try {
            return getStoreConnector(tableId, regionId).exec(
                stack, function, retryTimes, ErrorCodeUtils.errorToStrategyFunc
            );
        } catch (Exception e) {
            log.error(
                "Call [{}] exec error, table id: [{}], region id: [{}], msg: [{}].",
                stack, tableId, regionId, e.getMessage()
            );
            throw e;
        }
    }
}
