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
import io.dingodb.sdk.common.concurrent.Executors;
import io.dingodb.sdk.common.table.metric.TableMetrics;
import io.dingodb.sdk.common.utils.EntityConversion;
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

import static io.dingodb.sdk.common.utils.EntityConversion.mapping;

@Accessors(fluent = true)
public class MetaServiceClient {

    private static final Meta.DingoCommonId ROOT_SCHEMA_ID = Meta.DingoCommonId.newBuilder()
            .setEntityType(Meta.EntityType.ENTITY_TYPE_SCHEMA)
            .setEntityId(Meta.ReservedSchemaIds.ROOT_SCHEMA_VALUE)
            .setParentEntityId(0)
            .build();

    private static final Meta.DingoCommonId DINGO_SCHEMA_ID = Meta.DingoCommonId.newBuilder()
            .setEntityType(Meta.EntityType.ENTITY_TYPE_SCHEMA)
            .setEntityId(Meta.ReservedSchemaIds.DINGO_SCHEMA_VALUE)
            .setParentEntityId(0)
            .build();

    private final Map<String, Meta.DingoCommonId> metaServiceIdCache = new ConcurrentSkipListMap<>();
    private final Map<Meta.DingoCommonId, Table> tableDefinitionCache = new ConcurrentHashMap<>();
    private final Map<Meta.DingoCommonId, MetaServiceClient> metaServiceCache = new ConcurrentHashMap<>();
    private final Map<String, Meta.DingoCommonId> tableIdCache = new ConcurrentHashMap<>();

    private MetaServiceGrpc.MetaServiceBlockingStub metaBlockingStub;

    private String ROOT_NAME = "DINGO_ROOT";
    private Meta.DingoCommonId parentId;
    @Getter
    private Meta.DingoCommonId id;
    @Getter
    private final String name;

    private ServiceConnector connector;

    public MetaServiceClient(ServiceConnector connector) {
        this.parentId = ROOT_SCHEMA_ID;
        this.id = DINGO_SCHEMA_ID;
        this.name = ROOT_NAME;
        this.connector = connector;
        this.connector.initConnection();
        this.metaBlockingStub = this.connector.getMetaBlockingStub();
        Executors.execute("meta-service-client-reload", this::reload);
    }

    private MetaServiceClient(Meta.DingoCommonId id, String name, ServiceConnector connector) {
        this.parentId = ROOT_SCHEMA_ID;
        this.connector = connector;
        this.id = id;
        this.name = name;
        this.metaBlockingStub = connector.getMetaBlockingStub();
    }

    public void close() {
        connector.shutdown();
    }

    private synchronized void reload() {
        if (!tableDefinitionCache.isEmpty() || !metaServiceCache.isEmpty()) {
            return;
        }
        if (id == null) {
            id = DINGO_SCHEMA_ID;
        }
        this.getSchemas(parentId).forEach(this::addMetaServiceCache);
        this.getTableDefinitions(id).forEach(this::addTableCache);
    }

    private void addMetaServiceCache(Meta.Schema schema) {
        metaServiceIdCache.computeIfAbsent(schema.getName(), __ -> schema.getId());
        metaServiceCache.computeIfAbsent(
                schema.getId(), __ -> new MetaServiceClient(schema.getId(), schema.getName(), connector)
        );
    }

    public void createSubMetaService(String name) {
        Meta.CreateSchemaRequest request = Meta.CreateSchemaRequest.newBuilder()
                .setParentSchemaId(parentId)
                .setSchemaName(name)
                .build();

        Meta.CreateSchemaResponse response = metaBlockingStub.createSchema(request);
        Meta.Schema schema = response.getSchema();

        addMetaServiceCache(schema);
    }

    public List<Meta.Schema> getSchemas(Meta.DingoCommonId id) {
        Meta.GetSchemasRequest request = Meta.GetSchemasRequest.newBuilder()
                .setSchemaId(id)
                .build();

        Meta.GetSchemasResponse response = metaBlockingStub.getSchemas(request);
        return response.getSchemasList();
    }

    public Map<String, MetaServiceClient> getSubMetaServices() {
        return metaServiceCache.values().stream()
                .collect(Collectors.toMap(MetaServiceClient::name, Function.identity()));
    }

    public MetaServiceClient getSubMetaService(String name) {
        MetaServiceClient metaService = Optional.mapOrNull(metaServiceIdCache.get(name), metaServiceCache::get);
        if (metaService == null) {
            Meta.GetSchemaByNameRequest request = Meta.GetSchemaByNameRequest.newBuilder().setSchemaName(name).build();
            Meta.GetSchemaByNameResponse response = metaBlockingStub.getSchemaByName(request);
            if (response.getSchema().getName().isEmpty()) {
                return null;
            }
            metaService = Optional.ofNullable(response.getSchema())
                    .ifPresent(this::addMetaServiceCache)
                    .map(Meta.Schema::getId)
                    .mapOrNull(metaServiceCache::get);
        }
        return metaService;
    }

    public boolean dropSubMetaService(String name) {
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
        tableDefinitionCache.computeIfAbsent(tableId, __ -> mapping(tableDefinitionWithId.getTableDefinition()));
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

        Meta.TableDefinition definition = mapping(table, tableId);

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
            Meta.GetTableByNameRequest request = Meta.GetTableByNameRequest.newBuilder()
                    .setSchemaId(id)
                    .setTableName(tableName)
                    .build();
            Meta.GetTableByNameResponse response = metaBlockingStub.getTableByName(request);

            Meta.TableDefinitionWithId withId = response.getTableDefinitionWithId();
            if (withId.getTableDefinition().getName().equals(tableName)) {
                addTableCache(withId);
                tableId = tableIdCache.get(tableName);
            }
        }
        return tableId;
    }

    public Map<String, Table> getTableDefinitions() {
        if (tableDefinitionCache.isEmpty()) {
            reload();
        }
        return tableDefinitionCache.values().stream()
                .collect(Collectors.toMap(Table::getName, Function.identity()));
    }

    private List<Meta.TableDefinitionWithId> getTableDefinitions(Meta.DingoCommonId id) {
        Meta.GetTablesRequest request = Meta.GetTablesRequest.newBuilder()
                .setSchemaId(id)
                .build();

        Meta.GetTablesResponse response = metaBlockingStub.getTables(request);
        return response.getTableDefinitionWithIdsList();
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
            table = mapping(response.getTableDefinitionWithId().getTableDefinition());
        }
        return table;
    }

    public NavigableMap<ByteArrayUtils.ComparableByteArray, Meta.RangeDistribution> getRangeDistribution(String tableName) {
        Meta.DingoCommonId tableId = getTableId(tableName);
        return getRangeDistribution(tableId);
    }

    public NavigableMap<ByteArrayUtils.ComparableByteArray, Meta.RangeDistribution> getRangeDistribution(Meta.DingoCommonId id) {
        NavigableMap<ByteArrayUtils.ComparableByteArray, Meta.RangeDistribution> result = new TreeMap<>();
        Meta.GetTableRangeRequest request = Meta.GetTableRangeRequest.newBuilder()
                .setTableId(id)
                .build();

        Meta.GetTableRangeResponse response = metaBlockingStub.getTableRange(request);

        for (Meta.RangeDistribution tablePart : response.getTableRange().getRangeDistributionList()) {
            result.put(new ByteArrayUtils.ComparableByteArray(
                            tablePart.getRange().getStartKey().toByteArray()),
                    tablePart);
        }
        return result;
    }

    public TableMetrics getTableMetrics(String tableName) {
        return Optional.ofNullable(getTableId(tableName))
                .map(tableId -> {
                    Meta.GetTableMetricsRequest request = Meta.GetTableMetricsRequest.newBuilder()
                            .setTableId(tableId)
                            .build();
                    Meta.GetTableMetricsResponse response = metaBlockingStub.getTableMetrics(request);
                    Meta.TableMetricsWithId metrics = response.getTableMetrics();
                    return metrics.getTableMetrics();
                })
                .mapOrNull(EntityConversion::mapping);
    }
}