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

import io.dingodb.client.Key;
import io.dingodb.client.OperationService;
import io.dingodb.client.Record;
import io.dingodb.client.operation.DeleteOperation;
import io.dingodb.client.operation.DeleteRangeOperation;
import io.dingodb.client.operation.GetOperation;
import io.dingodb.client.operation.OpKeyRange;
import io.dingodb.client.operation.PutIfAbsentOperation;
import io.dingodb.client.operation.PutOperation;
import io.dingodb.client.operation.ScanOperation;
import io.dingodb.sdk.common.DingoClientException;
import io.dingodb.sdk.common.table.Table;
import io.dingodb.sdk.common.utils.Optional;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import static io.dingodb.sdk.service.connector.MetaServiceConnector.getMetaServiceConnector;

public class DingoClient {

    private final String schema;

    private OperationService operationService;

    public static Integer retryTimes = 20;

    public DingoClient(String coordinatorSvr) {
        this(coordinatorSvr, retryTimes);
    }

    public DingoClient(String coordinatorSvr, Integer retryTimes) {
        this(coordinatorSvr, "DINGO", retryTimes);
    }

    public DingoClient(String coordinatorSvr, String schema, Integer retryTimes) {
        operationService = new OperationService(getMetaServiceConnector(coordinatorSvr), retryTimes);
        this.schema = schema;
    }

    public boolean open() {
        operationService.init();
        return true;
    }

    public boolean createTable(Table table) {
        boolean isSuccess = operationService.createTable(schema, table.getName(), table);
        return isSuccess;
    }

    public boolean dropTable(String tableName) {
        boolean isSuccess = operationService.dropTable(schema, tableName);
        return isSuccess;
    }

    public boolean upsert(String tableName, Record record) {
        return upsert(tableName, Collections.singletonList(record)).get(0);
    }

    public List<Boolean> upsert(String tableName, List<Record> records) {
        return operationService.exec(schema, tableName, PutOperation.getInstance(), records);
    }

    public boolean putIfAbsent(String tableName, Record record) {
        return putIfAbsent(tableName, Collections.singletonList(record)).get(0);
    }

    public List<Boolean> putIfAbsent(String tableName, List<Record> records) {
        return operationService.exec(schema, tableName, PutIfAbsentOperation.getInstance(), records);
    }

    public Record get(String tableName, Key key) {
        List<Record> records = get(tableName, Collections.singletonList(key));
        if (records != null && records.size() > 0) {
            return records.get(0);
        }
        return null;
    }

    public List<Record> get(String tableName, List<Key> keys) {
        return operationService.exec(schema, tableName, GetOperation.getInstance(), keys);
    }

    public Record get(final String tableName, final Key firstKey, List<String> colNames) {
        return Optional.mapOrNull(get(tableName, firstKey), r -> r.extract(colNames));
    }

    public Iterator<Record> scan(final String tableName, Key begin, Key end, boolean withBegin, boolean withEnd) {
        return operationService.exec(
            schema, tableName, ScanOperation.getInstance(), new OpKeyRange(begin, end, withBegin, withEnd)
        );
    }

    public boolean delete(final String tableName, Key key) {
        return delete(tableName, Collections.singletonList(key)).get(0);
    }

    public List<Boolean> delete(final String tableName, List<Key> keys) {
        return operationService.exec(schema, tableName, DeleteOperation.getInstance(), keys);
    }

    public long delete(String tableName, Key begin, Key end, boolean withBegin, boolean withEnd) {
        return operationService.exec(
            schema,
            tableName,
            DeleteRangeOperation.getInstance(),
            new OpKeyRange(begin, end, withBegin, withEnd)
        );
    }

    public Table getTableDefinition(final String tableName) {
        if (tableName == null || tableName.isEmpty()) {
            throw new DingoClientException("Invalid table name: " + tableName);
        }
        return operationService.getTableDefinition(schema, tableName);
    }

    public void close() {
        operationService.close();
    }
}