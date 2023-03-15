/*
 * Copyright 2021, Zetyun DataPortal All rights reserved.
 */

package io.dingodb;

import io.dingodb.client.ContextForClient;
import io.dingodb.client.Result;
import io.dingodb.client.ServiceOperation;
import io.dingodb.client.StoreOperationType;
import io.dingodb.sdk.common.table.Table;

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
}