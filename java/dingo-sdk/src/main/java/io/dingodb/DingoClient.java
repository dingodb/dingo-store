/*
 * Copyright 2021, Zetyun DataPortal All rights reserved.
 */

package io.dingodb;

import io.dingodb.client.ContextForClient;
import io.dingodb.client.Result;
import io.dingodb.client.ServiceOperation;
import io.dingodb.client.StoreOperationType;
import io.dingodb.sdk.common.partition.PartitionDetailDefinition;
import io.dingodb.sdk.common.partition.PartitionRule;
import io.dingodb.sdk.common.table.ColumnDefinition;
import io.dingodb.sdk.common.table.Table;
import io.dingodb.sdk.common.table.TableDefinition;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class DingoClient {

    private UnifyStoreConnection connection;

    public ServiceOperation serviceOperation;

    public DingoClient(String coordinatorSvr, Integer retryTimes) {
        connection = new UnifyStoreConnection(coordinatorSvr, retryTimes);
    }

    public boolean open() {
        connection.initConnection();
        serviceOperation = new ServiceOperation(connection, 10);
        return true;
    }

    public boolean createTable(Table table) {
        return connection.getMetaClient().createTable(table.getName(), table);
    }

    public boolean dropTable(String tableName) {
        return connection.getMetaClient().dropTable(tableName);
    }

    public boolean upsert(String tableName, List<Object[]> keys, List<Object[]> values) {

        Result result = serviceOperation.operation(
                tableName,
                StoreOperationType.PUT,
                ContextForClient.builder().keyList(keys).values(values).build());

        return result.isSuccess();
    }                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     

    public Result get(String tableName, List<Object[]> keys) {
        Result result = serviceOperation.operation(
                tableName,
                StoreOperationType.GET,
                ContextForClient.builder().keyList(keys).build()
        );

        return result;
    }

    public static void main(String[] args) {
        DingoClient dingoClient = new DingoClient("192.168.1.201:22001", 10);
        dingoClient.open();

        String tableName = "test";

        boolean test1 = dingoClient.dropTable(tableName);
        System.out.println(test1);

        ColumnDefinition c1 = new ColumnDefinition("id", "integer", "", 0, 0, false, 0, "");
        ColumnDefinition c2 = new ColumnDefinition("name", "varchar", "", 0, 0, false, -1, "");

        PartitionDetailDefinition detailDefinition = new PartitionDetailDefinition(null, null, Arrays.asList(new Object[]{1}));
        PartitionRule partitionRule = new PartitionRule(null, null, Arrays.asList(detailDefinition));

        TableDefinition tableDefinition = new TableDefinition(tableName, Arrays.asList(c1, c2), 1, 0, partitionRule, ENG_ROCKSDB.name(), null);
        boolean isSuccess = dingoClient.createTable(tableDefinition);

        System.out.println(isSuccess);

        Object[] objects = {1};

        boolean test = dingoClient.upsert(tableName, Collections.singletonList(new Object[]{1, null}), Collections.singletonList(new Object[]{1, "zhangsan"}));
        System.out.println(test);

        Result result = dingoClient.get(tableName, Collections.singletonList(new Object[]{1, null}));

        for (Object[] value : result.getValues()) {
            System.out.println(Arrays.toString(value));
        }
    }
}