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

import io.dingodb.sdk.common.DingoClientException;
import io.dingodb.sdk.common.utils.Optional;
import io.dingodb.sdk.service.connector.ServiceConnector;
import io.dingodb.sdk.common.table.Table;
import io.dingodb.sdk.common.utils.ByteArrayUtils;
import io.dingodb.meta.Meta;
import io.dingodb.meta.MetaServiceGrpc;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;

import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.function.Function;
import java.util.stream.Collectors;

import static io.dingodb.sdk.common.utils.EntityConversion.swap;

@Accessors(fluent = true)
public class MetaServiceClient {

    public static final Meta.DingoCommonId DINGO_SCHEMA_ID = Meta.DingoCommonId.newBuilder()
            .setEntityType(Meta.EntityType.ENTITY_TYPE_SCHEMA)
            .setEntityId(Meta.ReservedSchemaIds.DINGO_SCHEMA_VALUE)
            .setParentEntityId(0)
            .build();

    private final Map<String, Meta.DingoCommonId> metaServiceIdCache = new ConcurrentSkipListMap<>();
    private final Map<Meta.DingoCommonId, Table> tableDefinitionCache = new ConcurrentHashMap<>();
    private final Map<Meta.DingoCommonId, MetaServiceClient> metaServiceCache = new ConcurrentSkipListMap<>();
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

    public MetaServiceClient(Meta.DingoCommonId id, String name, ServiceConnector connector) {
        this.connector = connector;
        this.id = id;
        this.name = name;
        // TODO reload
    }

    public void close() {
        connector.shutdown();
    }

    private void addMetaServiceCache(Meta.Schema schema) {
        metaServiceIdCache.computeIfAbsent(schema.getName(), __ -> schema.getId());
        metaServiceCache.computeIfAbsent(
                schema.getId(), __ -> new MetaServiceClient(schema.getId(), schema.getName(), connector)
        );
    }

    public void createSchema(String name) {
        Meta.CreateSchemaRequest request = Meta.CreateSchemaRequest.newBuilder()
                .setParentSchemaId(id)
                .setSchemaName(name)
                .build();

        Meta.CreateSchemaResponse response = metaBlockingStub.createSchema(request);
        Meta.Schema schema = response.getSchema();

        addMetaServiceCache(schema);
    }

    public Map<String, MetaServiceClient> getMetaServices() {
        return metaServiceCache.values().stream()
                .collect(Collectors.toMap(MetaServiceClient::name, Function.identity()));
    }

    public MetaServiceClient getMetaService(String name) {
        MetaServiceClient metaService = Optional.mapOrNull(metaServiceIdCache.get(name), metaServiceCache::get);
        if (metaService == null) {
            // TODO Get the schema from the store by name
        }
        return metaService;
    }

    public boolean dropSchema(String name) {
        return Optional.ofNullable(metaServiceIdCache.get(name))
                .map(schemaId -> {
                    Meta.DropSchemaRequest request = Meta.DropSchemaRequest.newBuilder()
                            .setSchemaId(metaServiceIdCache.get(name))
                            .build();
                    Meta.DropSchemaResponse response = metaBlockingStub.dropSchema(request);
                    return response.getError().getErrcodeValue() == 0;
                })
                .orElse(false);
    }

    private void addTableCache(Meta.TableDefinitionWithId tableDefinitionWithId) {
        Meta.DingoCommonId tableId = tableDefinitionWithId.getTableId();
        String name = tableDefinitionWithId.getTableDefinition().getName();
        tableIdCache.computeIfAbsent(name, __ -> tableId);
        tableDefinitionCache.computeIfAbsent(tableId, __ -> swap(tableDefinitionWithId.getTableDefinition()));
    }

    public boolean createTable(@NonNull String tableName, @NonNull Table table) {
        if (Optional.mapOrNull(getTableId(tableName), this::getTableDefinition) != null) {
            throw new DingoClientException("Table " + tableName + " already exists");
        }
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

    public synchronized boolean dropTable(@NonNull String tableName) {
        Meta.DingoCommonId tableId = getTableId(tableName);
        if (tableId == null) {
            throw new DingoClientException("Table " + tableName + " does not exist");
        }
        Meta.DropTableRequest request = Meta.DropTableRequest.newBuilder()
                .setTableId(tableId)
                .build();

        Meta.DropTableResponse response = metaBlockingStub.dropTable(request);
        tableIdCache.remove(tableName);
        tableDefinitionCache.remove(tableId);

        return response.getError().getErrcodeValue() == 0;
    }

    public Meta.DingoCommonId getTableId(@NonNull String tableName) {
        Meta.DingoCommonId tableId = tableIdCache.get(tableName);
        if (tableId == null) {
            Meta.GetTablesRequest request = Meta.GetTablesRequest.newBuilder().setSchemaId(id).build();
            Meta.GetTablesResponse response = metaBlockingStub.getTables(request);

            List<Meta.TableDefinitionWithId> withIdsList = response.getTableDefinitionWithIdsList();
            for (Meta.TableDefinitionWithId withId : withIdsList) {
                if (withId.getTableDefinition().getName().equalsIgnoreCase(tableName)) {
                    addTableCache(withId);
                    tableId = tableIdCache.get(tableName);
                    break;
                }
            }
        }
        return tableId;
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

    public Table getTableDefinition(@NonNull Meta.DingoCommonId tableId) {
        Table table = tableDefinitionCache.get(tableId);
        if (table == null) {
            Meta.GetTableRequest request = Meta.GetTableRequest.newBuilder().setTableId(tableId).build();
            Meta.GetTableResponse response = metaBlockingStub.getTable(request);
            table = swap(response.getTableDefinitionWithId().getTableDefinition());
        }
        return table;
    }

    public NavigableMap<ByteArrayUtils.ComparableByteArray, Meta.RangeDistribution> getParts(String tableName) {
        Meta.DingoCommonId tableId = getTableId(tableName);
        return getParts(tableId);
    }

    public NavigableMap<ByteArrayUtils.ComparableByteArray, Meta.RangeDistribution> getParts(Meta.DingoCommonId id) {
        NavigableMap<ByteArrayUtils.ComparableByteArray, Meta.RangeDistribution> result = new TreeMap<>();
        Meta.GetTableRangeRequest request = Meta.GetTableRangeRequest.newBuilder()
                .setTableId(id)
                .build();

        Meta.GetTableRangeResponse response = metaBlockingStub.getTableRange(request);

        for (Meta.RangeDistribution tablePart : response.getTableRange().getRangeDistributionList() ) {
            result.put(new ByteArrayUtils.ComparableByteArray(
                    tablePart.getRange().getStartKey().toByteArray()),
                    tablePart);
        }
        return result;
    }
}