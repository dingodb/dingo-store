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

package io.dingodb;

import io.dingodb.client.ContextForClient;
import io.dingodb.client.Key;
import io.dingodb.client.Record;
import io.dingodb.client.Result;
import io.dingodb.client.ServiceOperation;
import io.dingodb.client.operation.StoreOperationType;
import io.dingodb.client.Value;
import io.dingodb.sdk.common.table.Table;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

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

    public boolean upsert(String tableName, List<Record> records) {
        List<Key> keys = getKeys(tableName, records);
        Result result = serviceOperation.operation(
                tableName,
                StoreOperationType.PUT,
                ContextForClient.builder().keyList(keys).records(records).build());

        return result.isSuccess();
    }

    public boolean putIfAbsent(final String tableName, List<Record> records) {
        List<Key> keys = getKeys(tableName, records);
        Result result = serviceOperation.operation(
                tableName,
                StoreOperationType.PUT_IF_ABSENT,
                ContextForClient.builder().keyList(keys).records(records).build());
        return result.isSuccess();
    }

    private List<Key> getKeys(String tableName, List<Record> records) {
        List<Key> keys = records.stream()
                .map(__ ->
                        new Key(tableName, __.getKeyValues().stream()
                                .map(Value::get)
                                .collect(Collectors.toList())))
                .collect(Collectors.toList());
        return keys;
    }

    public Record get(String tableName, Key key) {
        List<Record> records = get(tableName, Collections.singletonList(key));
        if (records != null && records.size() > 0) {
            return records.get(0);
        }
        return null;
    }

    public List<Record> get(String tableName, List<Key> keys) {
        Result result = serviceOperation.operation(
                tableName,
                StoreOperationType.GET,
                ContextForClient.builder().keyList(keys).build()
        );
        return result.getValues();
    }

    public Record get(final String tableName, final Key firstKey, List<String> colNames) {
        Record record = get(tableName, firstKey);

        if (record == null) {
            return null;
        }
        return Record.toRecordByColumn(record, colNames);
    }

    public boolean delete(final String tableName, Key key) {
        return delete(tableName, Collections.singletonList(key));
    }

    public boolean delete(final String tableName, List<Key> keys) {
        Result result = serviceOperation.operation(
                tableName,
                StoreOperationType.DELETE,
                ContextForClient.builder().keyList(keys).build()
        );
        return result.isSuccess();
    }

    public void close() {
        serviceOperation.close();
    }
}