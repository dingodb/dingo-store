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

package io.dingodb.sdk.service.meta;

import io.dingodb.sdk.service.connector.ServiceConnector;
import io.dingodb.sdk.common.table.Table;
import io.dingodb.sdk.common.utils.ByteArrayUtils;
import io.dingodb.meta.Meta;
import io.dingodb.meta.MetaServiceGrpc;
import lombok.Getter;
import lombok.NonNull;

import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;

import static io.dingodb.sdk.common.utils.EntityConversion.swap;

public class MetaServiceClient {

    public static final Meta.DingoCommonId DINGO_SCHEMA_ID = Meta.DingoCommonId.newBuilder()
            .setEntityType(Meta.EntityType.ENTITY_TYPE_SCHEMA)
            .setEntityId(Meta.ReservedSchemaIds.DINGO_SCHEMA_VALUE)
            .setParentEntityId(0)
            .build();

    private final Map<Meta.DingoCommonId, Table> tableDefinitionCache = new ConcurrentHashMap<>();
    private final Map<String, Meta.DingoCommonId> tableIdCache = new ConcurrentHashMap<>();

    private MetaServiceGrpc.MetaServiceBlockingStub metaBlockingStub;

    private String ROOT_NAME = "DINGO_ROOT";
    @Getter
    private Meta.DingoCommonId id;
    @Getter
    private final String name;

    private ServiceConnector connector;

    public MetaServiceClient(ServiceConnector connector) {
        this.id = DINGO_SCHEMA_ID;
        this.name = ROOT_NAME;
        this.connector = connector;
        this.connector.initConnection();
        this.metaBlockingStub = this.connector.getMetaBlockingStub();
    }

    public void close() {
        connector.shutdown();
    }

    public boolean createTable(@NonNull String tableName, @NonNull Table table) {
        Meta.CreateTableIdRequest createTableIdRequest = Meta.CreateTableIdRequest.newBuilder()
                .setSchemaId(id)
                .build();

        Meta.CreateTableIdResponse createTableIdResponse = metaBlockingStub.createTableId(createTableIdRequest);
        Meta.DingoCommonId tableId = createTableIdResponse.getTableId();

        Meta.TableDefinition definition = swap(table, tableId);

        Meta.CreateTableRequest request = Meta.CreateTableRequest.newBuilder()
                .setSchemaId(id)
                .setTableId(tableId)
                .setTableDefinition(definition)
                .build();

        Meta.CreateTableResponse response = metaBlockingStub.createTable(request);

        tableIdCache.put(tableName, tableId);
        tableDefinitionCache.put(tableId, table);

        return response.getError().getErrcodeValue() == 0;
    }

    public boolean dropTable(@NonNull String tableName) {
        Meta.DropTableRequest request = Meta.DropTableRequest.newBuilder()
                .setTableId(getTableId(tableName))
                .build();

        Meta.DropTableResponse response = metaBlockingStub.dropTable(request);

        return response.getError().getErrcodeValue() == 0;
    }

    public Meta.DingoCommonId getTableId(@NonNull String tableName) {
        Meta.DingoCommonId commonId = tableIdCache.get(tableName);
        if (commonId == null) {
            Meta.GetTablesRequest request = Meta.GetTablesRequest.newBuilder().setSchemaId(id).build();
            Meta.GetTablesResponse response = metaBlockingStub.getTables(request);

            List<Meta.TableDefinitionWithId> withIdsList = response.getTableDefinitionWithIdsList();
            for (Meta.TableDefinitionWithId withId : withIdsList) {
                if (withId.getTableDefinition().getName().equalsIgnoreCase(tableName)) {
                    commonId = withId.getTableId();
                    tableDefinitionCache.put(commonId, swap(withId.getTableDefinition()));
                    tableIdCache.put(tableName, commonId);
                    break;
                }
            }
        }
        return commonId;
    }

    public Map<String, Table> getTableDefinitions() {
        if (tableDefinitionCache.isEmpty()) {
            // TODO reload
        }
        return tableDefinitionCache.values().stream()
                .collect(Collectors.toMap(Table::getName, Function.identity()));
    }

    public Table getTableDefinition(@NonNull String name) {
        Meta.DingoCommonId tableId = getTableId(name);
        return getTableDefinition(tableId);
    }

    public Table getTableDefinition(@NonNull Meta.DingoCommonId id) {
        return tableDefinitionCache.get(id);
    }

    public NavigableMap<ByteArrayUtils.ComparableByteArray, Meta.Part> getParts(String tableName) {
        Meta.DingoCommonId tableId = getTableId(tableName);
        return getParts(tableId);
    }

    public NavigableMap<ByteArrayUtils.ComparableByteArray, Meta.Part> getParts(Meta.DingoCommonId id) {
        NavigableMap<ByteArrayUtils.ComparableByteArray, Meta.Part> result = new TreeMap<>();
        Meta.GetTableRequest request = Meta.GetTableRequest.newBuilder()
                .setTableId(id)
                .build();

        Meta.GetTableResponse response = metaBlockingStub.getTable(request);

        for (Meta.Part tablePart : response.getTable().getPartsList()) {
            result.put(new ByteArrayUtils.ComparableByteArray(
                    tablePart.getRange().getStartKey().toByteArray()),
                    tablePart);
        }
        return result;
    }
}