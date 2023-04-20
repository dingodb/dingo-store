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

import io.dingodb.meta.Meta;
import io.dingodb.meta.MetaServiceGrpc;
import io.dingodb.sdk.common.DingoClientException;
import io.dingodb.sdk.common.DingoCommonId;
import io.dingodb.sdk.common.concurrent.Executors;
import io.dingodb.sdk.common.table.RangeDistribution;
import io.dingodb.sdk.common.table.Table;
import io.dingodb.sdk.common.table.metric.TableMetrics;
import io.dingodb.sdk.common.utils.ByteArrayUtils;
import io.dingodb.sdk.common.utils.EntityConversion;
import io.dingodb.sdk.common.utils.Optional;
import io.dingodb.sdk.service.connector.ServiceConnector;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static io.dingodb.sdk.common.utils.EntityConversion.mapping;

@Slf4j
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
    private final Map<DingoCommonId, Table> tableDefinitionCache = new ConcurrentHashMap<>();
    private final Map<Meta.DingoCommonId, MetaServiceClient> metaServiceCache = new ConcurrentHashMap<>();
    private final Map<String, Meta.DingoCommonId> tableIdCache = new ConcurrentHashMap<>();

    private Pattern pattern = Pattern.compile("^[A-Z_][A-Z\\d_]+$");
    private Pattern warnPattern = Pattern.compile(".*[a-z]+.*");
    private String ROOT_NAME = "DINGO_ROOT";
    private Meta.DingoCommonId parentId;
    @Getter
    private Meta.DingoCommonId id;
    @Getter
    private final String name;

    private ServiceConnector<MetaServiceGrpc.MetaServiceBlockingStub> connector;

    public MetaServiceClient(ServiceConnector<MetaServiceGrpc.MetaServiceBlockingStub> connector) {
        this.parentId = ROOT_SCHEMA_ID;
        this.id = ROOT_SCHEMA_ID;
        this.name = ROOT_NAME;
        this.connector = connector;
        Executors.execute("meta-service-client-reload", this::reload);
    }

    private MetaServiceClient(
            Meta.DingoCommonId id,
            String name,
            ServiceConnector<MetaServiceGrpc.MetaServiceBlockingStub> connector) {
        this.parentId = ROOT_SCHEMA_ID;
        this.connector = connector;
        this.id = id;
        this.name = name;
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

        Meta.Schema schema = connector.exec(stub -> {
            Meta.CreateSchemaResponse res = stub.createSchema(request);
            return new ServiceConnector.Response<>(res.getError(), res);
        }).getResponse().getSchema();

        addMetaServiceCache(schema);
    }

    public List<Meta.Schema> getSchemas(Meta.DingoCommonId id) {
        Meta.GetSchemasRequest request = Meta.GetSchemasRequest.newBuilder()
                .setSchemaId(id)
                .build();
        Meta.GetSchemasResponse response = connector.exec(stub -> {
            Meta.GetSchemasResponse res = stub.getSchemas(request);
            return new ServiceConnector.Response<>(res.getError(), res);
        }).getResponse();

        return response.getSchemasList();
    }

    public Map<String, MetaServiceClient> getSubMetaServices() {
        return metaServiceCache.values().stream()
                .collect(Collectors.toMap(MetaServiceClient::name, Function.identity()));
    }

    public MetaServiceClient getSubMetaService(String name) {
        Meta.DingoCommonId schemaId = metaServiceIdCache.get(name);
        MetaServiceClient metaService;
        if (schemaId == null) {
            Meta.GetSchemaByNameRequest request = Meta.GetSchemaByNameRequest.newBuilder().setSchemaName(name).build();

            Meta.GetSchemaByNameResponse response = connector.exec(stub -> {
                Meta.GetSchemaByNameResponse res = stub.getSchemaByName(request);
                return new ServiceConnector.Response<>(res.getError(), res);
            }).getResponse();

            if (response.getSchema().getName().isEmpty()) {
                return null;
            }
            metaService = Optional.ofNullable(response.getSchema())
                .ifPresent(this::addMetaServiceCache)
                .map(Meta.Schema::getId)
                .mapOrNull(metaServiceCache::get);
        } else {
            metaService = getSubMetaService(schemaId);
        }
        return metaService;
    }

    public MetaServiceClient getSubMetaService(DingoCommonId schemaId) {
        return getSubMetaService(Meta.DingoCommonId.newBuilder()
            .setEntityType(Meta.EntityType.ENTITY_TYPE_SCHEMA)
            .setParentEntityId(schemaId.parentId())
            .setEntityId(schemaId.entityId())
            .build());
    }

    private MetaServiceClient getSubMetaService(Meta.DingoCommonId schemaId) {
        return metaServiceCache.get(schemaId);
    }

    public boolean dropSubMetaService(String name) {
        return Optional.ofNullable(metaServiceIdCache.get(name))
                .map(schemaId -> {
                    Meta.DropSchemaRequest request = Meta.DropSchemaRequest.newBuilder()
                            .setSchemaId(metaServiceIdCache.get(name))
                            .build();
                    Meta.DropSchemaResponse response = connector.exec(stub -> {
                        Meta.DropSchemaResponse res = stub.dropSchema(request);
                        return new ServiceConnector.Response<>(res.getError(), res);
                    }).getResponse();
                    return response.getError().getErrcodeValue() == 0;
                })
                .orElse(false);
    }

    private void addTableCache(Meta.TableDefinitionWithId tableDefinitionWithId) {
        Meta.DingoCommonId tableId = tableDefinitionWithId.getTableId();
        String name = tableDefinitionWithId.getTableDefinition().getName();
        tableIdCache.computeIfAbsent(name, __ -> tableId);
        tableDefinitionCache.computeIfAbsent(mapping(tableId), __ -> mapping(tableDefinitionWithId.getTableDefinition()));
    }

    public boolean createTable(@NonNull String tableName, @NonNull Table table) {
        tableName = cleanTableName(tableName);
        if (Optional.mapOrNull(getTableId(tableName), this::getTableDefinition) != null) {
            throw new DingoClientException("Table " + tableName + " already exists");
        }
        Meta.CreateTableIdRequest createTableIdRequest = Meta.CreateTableIdRequest.newBuilder()
                .setSchemaId(id)
                .build();

        Meta.CreateTableIdResponse createTableIdResponse = connector.exec(stub -> {
            Meta.CreateTableIdResponse res = stub.createTableId(createTableIdRequest);
            return new ServiceConnector.Response<>(res.getError(), res);
        }).getResponse();
        Meta.DingoCommonId tableId = createTableIdResponse.getTableId();

        Meta.TableDefinition definition = mapping(table, tableId);

        Meta.CreateTableRequest request = Meta.CreateTableRequest.newBuilder()
                .setSchemaId(id)
                .setTableId(tableId)
                .setTableDefinition(definition)
                .build();

        Meta.CreateTableResponse response = connector.exec(stub -> {
            Meta.CreateTableResponse res = stub.createTable(request);
            return new ServiceConnector.Response<>(res.getError(), res);
        }).getResponse();


        tableIdCache.put(tableName, tableId);
        tableDefinitionCache.put(mapping(tableId), table);

        return response.getError().getErrcodeValue() == 0;
    }

    public synchronized boolean dropTable(@NonNull String tableName) {
        tableName = cleanTableName(tableName);
        DingoCommonId tableId = getTableId(tableName);
        if (tableId == null) {
            throw new DingoClientException("Table " + tableName + " does not exist");
        }
        Meta.DropTableRequest request = Meta.DropTableRequest.newBuilder()
                .setTableId(mapping(tableId))
                .build();

        Meta.DropTableResponse response = connector.exec(stub -> {
            Meta.DropTableResponse res = stub.dropTable(request);
            return new ServiceConnector.Response<>(res.getError(), res);
        }).getResponse();
        tableIdCache.remove(tableName);
        tableDefinitionCache.remove(tableId);

        return response.getError().getErrcodeValue() == 0;
    }

    public DingoCommonId getTableId(@NonNull String tableName) {
        tableName = cleanTableName(tableName);
        Meta.DingoCommonId tableId = tableIdCache.get(tableName);
        if (tableId == null) {
            Meta.GetTableByNameRequest request = Meta.GetTableByNameRequest.newBuilder()
                    .setSchemaId(id)
                    .setTableName(tableName)
                    .build();

            Meta.GetTableByNameResponse response = connector.exec(stub -> {
                Meta.GetTableByNameResponse res = stub.getTableByName(request);
                return new ServiceConnector.Response<>(res.getError(), res);
            }).getResponse();

            Meta.TableDefinitionWithId withId = response.getTableDefinitionWithId();
            if (withId.getTableDefinition().getName().equals(tableName)) {
                addTableCache(withId);
                tableId = tableIdCache.get(tableName);
            }
        }
        return Optional.mapOrNull(tableId, EntityConversion::mapping);
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

        Meta.GetTablesResponse response = connector.exec(stub -> {
            Meta.GetTablesResponse res = stub.getTables(request);
            return new ServiceConnector.Response<>(res.getError(), res);
        }).getResponse();
        return response.getTableDefinitionWithIdsList();
    }

    public Table getTableDefinition(@NonNull String tableName) {
        cleanTableName(tableName);
        DingoCommonId tableId = getTableId(tableName);
        if (tableId == null) {
            throw new DingoClientException("Table " + tableName + " does not exist");
        }
        return getTableDefinition(tableId);
    }

    public Table getTableDefinition(@NonNull DingoCommonId tableId) {
        Table table = tableDefinitionCache.get(tableId);
        if (table == null) {
            Meta.GetTableRequest request = Meta.GetTableRequest.newBuilder().setTableId(mapping(tableId)).build();
            Meta.GetTableResponse response = connector.exec(stub -> {
                Meta.GetTableResponse res = stub.getTable(request);
                return new ServiceConnector.Response<>(res.getError(), res);
            }).getResponse();
            table = mapping(response.getTableDefinitionWithId().getTableDefinition());
        }
        return table;
    }

    public NavigableMap<ByteArrayUtils.ComparableByteArray, RangeDistribution> getRangeDistribution(String tableName) {
        tableName = cleanTableName(tableName);
        DingoCommonId tableId = getTableId(tableName);
        if (tableId == null) {
            throw new DingoClientException("Table " + tableName + " does not exist");
        }
        return getRangeDistribution(tableId);
    }

    public NavigableMap<ByteArrayUtils.ComparableByteArray, RangeDistribution> getRangeDistribution(DingoCommonId id) {
        NavigableMap<ByteArrayUtils.ComparableByteArray, RangeDistribution> result = new TreeMap<>();
        Meta.GetTableRangeRequest request = Meta.GetTableRangeRequest.newBuilder()
                .setTableId(mapping(id))
                .build();

        Meta.GetTableRangeResponse response = connector.exec(stub -> {
            Meta.GetTableRangeResponse res = stub.getTableRange(request);
            return new ServiceConnector.Response<>(res.getError(), res);
        }).getResponse();

        for (Meta.RangeDistribution tablePart : response.getTableRange().getRangeDistributionList()) {
            result.put(new ByteArrayUtils.ComparableByteArray(
                            tablePart.getRange().getStartKey().toByteArray()),
                    mapping(tablePart));
        }
        return result;
    }

    public TableMetrics getTableMetrics(String tableName) {
        tableName = cleanTableName(tableName);
        DingoCommonId tableId = getTableId(tableName);
        if (tableId == null) {
            throw new DingoClientException("Table " + tableName + " does not exist");
        }
        return Optional.ofNullable(tableId)
                .map(__ -> {
                    Meta.GetTableMetricsRequest request = Meta.GetTableMetricsRequest.newBuilder()
                            .setTableId(mapping(__))
                            .build();
                    Meta.GetTableMetricsResponse response = connector.exec(stub -> {
                        Meta.GetTableMetricsResponse res = stub.getTableMetrics(request);
                        return new ServiceConnector.Response<>(res.getError(), res);
                    }).getResponse();
                    Meta.TableMetricsWithId metrics = response.getTableMetrics();
                    return metrics.getTableMetrics();
                })
                .mapOrNull(EntityConversion::mapping);
    }

    private String cleanTableName(String tableName) {
        if (warnPattern.matcher(tableName).matches()) {
            log.warn("Table name currently only supports uppercase letters, LowerCase -> UpperCase");
            tableName = tableName.toUpperCase();
        }
        if (!pattern.matcher(tableName).matches()) {
            throw new DingoClientException("Table name currently only supports uppercase letters, digits, and underscores");
        }
        return tableName;
    }
}
