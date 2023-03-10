/*
 * Copyright 2021, Zetyun DataPortal All rights reserved.
 */

package io.dingodb.client;

import io.dingodb.UnifyStoreConnection;
import io.dingodb.sdk.common.KeyValue;
import io.dingodb.sdk.common.codec.DingoKeyValueCodec;
import io.dingodb.sdk.common.codec.KeyValueCodec;
import io.dingodb.sdk.service.store.StoreServiceClient;
import io.dingodb.common.Common;
import io.dingodb.sdk.common.concurrent.Executors;
import io.dingodb.sdk.common.partition.RangeStrategy;
import io.dingodb.sdk.service.meta.MetaClient;
import io.dingodb.sdk.common.table.Table;
import io.dingodb.sdk.common.utils.ByteArrayUtils;
import io.dingodb.meta.Meta;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

@Slf4j
public class ServiceOperation {

    private static Map<String, RouteTable> dingoRouteTables = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
    private static Map<String, Table> tableDefinitionInCache = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

    private UnifyStoreConnection connection;
    private int retryTimes;

    public ServiceOperation(UnifyStoreConnection connection, int retryTimes) {
        this.connection = connection;
        this.retryTimes = retryTimes;
    }

    public Result operation(String tableName, StoreOperationType type, ContextForClient clientParameters) {
        RouteTable routeTable = getAndRefreshRouteTable(tableName, false);
        if (routeTable == null) {
            log.error("table {} not found when do operation:{}", tableName, type);
        }
        KeyValueCodec codec = routeTable.getCodec();
        Table tableDef = getTableDefinition(tableName);
        IStoreOperation storeOperation = OperationFactory.getStoreOperation(type);
        ContextForStore contextForStore = getStoreContext(clientParameters, codec, tableDef);
        Map<String, ContextForStore> keys2Store = groupKeysByStore(routeTable, tableName, contextForStore);

        List<Future<ResultForStore>> futures = new ArrayList<>();

        for (Map.Entry<String, ContextForStore> entry : keys2Store.entrySet()) {
            String leaderAddress = entry.getKey();
            StoreServiceClient serviceClient = getStore(routeTable, leaderAddress);
            futures.add(
                    Executors.submit("do-operation",
                            () -> storeOperation.doOperation(serviceClient, entry.getValue()))
            );
        }
        List<Object[]> results = new ArrayList<>();

        int code = 0;
        String message = null;

        for (Future<ResultForStore> future : futures) {
            try {
                ResultForStore resultForStore = future.get();
                code = resultForStore.getCode();
                message = resultForStore.getErrorMessage();
                List<Common.KeyValue> records = resultForStore.getRecords();
                results.addAll(records.stream()
                    .map(kv -> {
                        try {
                            return codec.decode(new KeyValue(kv.getKey().toByteArray(), kv.getValue().toByteArray()));
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    }).collect(Collectors.toList()));

            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            }
        }
        return new Result(code == 0, message, null, results);
    }

    private Map<String, ContextForStore> groupKeysByStore(
            RouteTable routeTable, String tableName, ContextForStore wholeContext) {
        Map<String, List<byte[]>> keyListByStore = new TreeMap<>();
        Map<String, Meta.DingoCommonId> regionIdMap = new HashMap<>();
        for (int index = 0; index < wholeContext.getStartKeyInBytes().size(); index++) {
            byte[] keyInBytes = wholeContext.getStartKeyInBytes().get(index);
            Meta.Part part = getPartByStartKey(routeTable, keyInBytes);
            String leaderAddress = part.getLeader().getHost() + ":" + part.getLeader().getPort();
            if (leaderAddress == null) {
                log.error("Cannot find partition, table {} key:{} not found when do operation",
                        tableName,
                        Arrays.toString(keyInBytes));
                throw new RuntimeException("table " + tableName + " key:" + Arrays.toString(keyInBytes)
                        + " not found when do operation");
            }
            List<byte[]> keyList = keyListByStore.computeIfAbsent(leaderAddress, k -> new ArrayList<>());
            regionIdMap.computeIfAbsent(leaderAddress, k -> part.getId());
            keyList.add(keyInBytes);
        }

        Map<String, ContextForStore> contextGroupByStore = new TreeMap<>();
        for (Map.Entry<String, List<byte[]>> entry : keyListByStore.entrySet()) {
            String leaderAddress = entry.getKey();
            List<byte[]> keys = entry.getValue();
            List<KeyValue> records = new java.util.ArrayList<>();
            for (byte[] key : keys) {
                records.add(wholeContext.getRecordByKey(key));
            }
            ContextForStore subStoreContext = ContextForStore.builder()
                    .startKeyInBytes(keys)
                    .recordList(records)
                    .regionId(regionIdMap.get(leaderAddress))
                    .build();
            contextGroupByStore.put(leaderAddress, subStoreContext);
        }

        return contextGroupByStore;
    }

    public synchronized RouteTable getAndRefreshRouteTable(final String tableName, boolean isRefresh) {
        if (isRefresh) {
            dingoRouteTables.remove(tableName);
        }
        RouteTable routeTable = dingoRouteTables.get(tableName);
        if (routeTable == null) {
            MetaClient metaClient = connection.getMetaClient();
            Table table = metaClient.getTableDefinition(tableName);

            NavigableMap<ByteArrayUtils.ComparableByteArray, Meta.Part> parts =
                    metaClient.getParts(table.getName());

            DingoKeyValueCodec keyValueCodec = new DingoKeyValueCodec(
                    table.getDingoType(),
                    table.getKeyMapping()
            );

            RangeStrategy rangeStrategy = new RangeStrategy(parts.navigableKeySet());
            Meta.DingoCommonId tableId = metaClient.getTableId(table.getName());

            routeTable = new RouteTable(tableId, keyValueCodec, parts, rangeStrategy);

            dingoRouteTables.put(tableName, routeTable);
        }

        return routeTable;
    }

    public synchronized Table getTableDefinition(String tableName) {
        Table tableDef = tableDefinitionInCache.get(tableName);
        if (tableDef == null) {
            MetaClient metaClient = connection.getMetaClient();
            tableDef = metaClient.getTableDefinition(tableName);
            if (tableDef != null) {
                tableDefinitionInCache.put(tableName, tableDef);
            }
        }
        if (tableDef == null) {
            log.error("Cannot find table:{} definition from meta", tableName);
        }

        return tableDef;
    }

    private ContextForStore getStoreContext(ContextForClient inputContext, KeyValueCodec codec, Table tableDef) {
        List<byte[]> startKeyInBytes = inputContext.getKeyList().stream().map(x -> {
            try {
                if (x.length != tableDef.getPrimaryKeyCount()) {
                    log.error("Inconsistent number of primary keys:{}", x);
                }
                return codec.encodeKey(x);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }).collect(Collectors.toList());

        List<KeyValue> keyValueList = inputContext.getValues().stream().map(x -> {
            try {
                return codec.encode(x);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }).collect(Collectors.toList());

        return ContextForStore.builder()
                .startKeyInBytes(startKeyInBytes)
                .recordList(keyValueList)
                .build();
    }

    private synchronized StoreServiceClient getStore(final RouteTable routeTable, String leaderAddress) {
        return routeTable.getLeaderStoreService(leaderAddress);
    }

    private synchronized Meta.Part getPartByStartKey(final RouteTable routeTable, byte[] keyInBytes) {
        return routeTable.getPartByKey(keyInBytes);
    }
}
