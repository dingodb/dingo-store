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
import io.dingodb.sdk.common.table.RangeDistribution;
import io.dingodb.sdk.common.table.Table;
import io.dingodb.sdk.common.table.metric.TableMetrics;
import io.dingodb.sdk.common.utils.ByteArrayUtils;
import io.dingodb.sdk.common.utils.EntityConversion;
import io.dingodb.sdk.common.utils.Optional;
import io.dingodb.sdk.service.connector.MetaServiceConnector;
import io.dingodb.sdk.service.connector.ServiceConnector;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static io.dingodb.sdk.common.utils.EntityConversion.mapping;

@Slf4j
@Accessors(fluent = true)
public class MetaServiceClient {

    private static final ExecutorService reloadExecutor = Executors.newCachedThreadPool(
        runnable -> new Thread(Thread.currentThread().getThreadGroup(), runnable, "meta-service-client-reload")
    );

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

    private static Pattern pattern = Pattern.compile("^[A-Z_][A-Z\\d_]+$");
    private static Pattern warnPattern = Pattern.compile(".*[a-z]+.*");
    private static final String ROOT_NAME = "root";

    private final Map<String, Meta.DingoCommonId> metaServiceIdCache = new ConcurrentSkipListMap<>();
    private final Map<DingoCommonId, Table> tableDefinitionCache = new ConcurrentHashMap<>();
    private final Map<Meta.DingoCommonId, MetaServiceClient> metaServiceCache = new ConcurrentHashMap<>();
    private final Map<String, Meta.DingoCommonId> tableIdCache = new ConcurrentHashMap<>();
    private final Map<DingoCommonId, TableMetrics> tableMetricsCache = new ConcurrentHashMap<>();

    private final Meta.DingoCommonId parentId;
    @Getter
    private final Meta.DingoCommonId id;
    @Getter
    private final String name;

    private Long count = 10000L;
    private Integer increment = 1;
    private Integer offset = 1;

    private ServiceConnector<MetaServiceGrpc.MetaServiceBlockingStub> metaConnector;

    public MetaServiceClient(String servers) {
        this.parentId = ROOT_SCHEMA_ID;
        this.id = ROOT_SCHEMA_ID;
        this.name = ROOT_NAME;
        this.metaConnector = MetaServiceConnector.getMetaServiceConnector(servers);
        // TODO reloadExecutor.execute(this::reload);
    }

    @Deprecated
    public MetaServiceClient(ServiceConnector<MetaServiceGrpc.MetaServiceBlockingStub> metaConnector) {
        this.parentId = ROOT_SCHEMA_ID;
        this.id = ROOT_SCHEMA_ID;
        this.name = ROOT_NAME;
        this.metaConnector = metaConnector;
        // TODO reloadExecutor.execute(this::reload);
    }

    private MetaServiceClient(
            Meta.DingoCommonId id,
            String name,
            ServiceConnector<MetaServiceGrpc.MetaServiceBlockingStub> metaConnector) {
        this.parentId = ROOT_SCHEMA_ID;
        this.metaConnector = metaConnector;
        this.id = id;
        this.name = name;
    }

    public ServiceConnector<MetaServiceGrpc.MetaServiceBlockingStub> getMetaConnector() {
        return metaConnector;
    }

    public void close() {
    }

    private synchronized void reload() {
        if (!tableDefinitionCache.isEmpty() || !metaServiceCache.isEmpty()) {
            return;
        }
        this.getSchemas(parentId).forEach(this::addMetaServiceCache);
        if (!(id == ROOT_SCHEMA_ID)) {
            this.getTableDefinitions(id).forEach(this::addTableCache);
        }
    }

    private void addMetaServiceCache(Meta.Schema schema) {
        metaServiceIdCache.computeIfAbsent(schema.getName(), __ -> schema.getId());
        metaServiceCache.computeIfAbsent(schema.getId(),
                __ -> new MetaServiceClient(schema.getId(), schema.getName(), metaConnector)
        );
    }

    public void createSubMetaService(String name) {
        Meta.CreateSchemaRequest request = Meta.CreateSchemaRequest.newBuilder()
                .setParentSchemaId(parentId)
                .setSchemaName(name)
                .build();

        Meta.Schema schema = metaConnector.exec(stub -> {
            Meta.CreateSchemaResponse res = stub.createSchema(request);
            return new ServiceConnector.Response<>(res.getError(), res);
        }).getResponse().getSchema();

        // TODO addMetaServiceCache(schema);
    }

    public List<Meta.Schema> getSchemas(Meta.DingoCommonId id) {
        Meta.GetSchemasRequest request = Meta.GetSchemasRequest.newBuilder()
                .setSchemaId(id)
                .build();
        Meta.GetSchemasResponse response = metaConnector.exec(stub -> {
            Meta.GetSchemasResponse res = stub.getSchemas(request);
            return new ServiceConnector.Response<>(res.getError(), res);
        }).getResponse();

        return response.getSchemasList();
    }

    public Map<String, MetaServiceClient> getSubMetaServices() {
        /* TODO
            return metaServiceCache.values().stream()
                .collect(Collectors.toMap(MetaServiceClient::name, Function.identity()));*/
        return getSchemas(parentId).stream()
                .map(schema -> new MetaServiceClient(schema.getId(), schema.getName(), metaConnector))
                .collect(Collectors.toMap(MetaServiceClient::name, Function.identity()));
    }

    public MetaServiceClient getSubMetaService(String name) {
        /* TODO
        Meta.DingoCommonId schemaId = metaServiceIdCache.get(name);
        MetaServiceClient metaService;
        if (schemaId == null) {
            Meta.GetSchemaByNameRequest request = Meta.GetSchemaByNameRequest.newBuilder().setSchemaName(name).build();

            Meta.GetSchemaByNameResponse response = metaConnector.exec(stub -> {
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
        }*/
        Meta.GetSchemaByNameRequest request = Meta.GetSchemaByNameRequest.newBuilder().setSchemaName(name).build();

        Meta.GetSchemaByNameResponse response = metaConnector.exec(stub -> {
            Meta.GetSchemaByNameResponse res = stub.getSchemaByName(request);
            return new ServiceConnector.Response<>(res.getError(), res);
        }).getResponse();

        Meta.Schema schema = response.getSchema();
        if (schema.getName().isEmpty()) {
            return null;
        }
        return new MetaServiceClient(schema.getId(), schema.getName(), metaConnector);
    }

    public MetaServiceClient getSubMetaService(DingoCommonId schemaId) {
        return getSubMetaService(Meta.DingoCommonId.newBuilder()
            .setEntityType(Meta.EntityType.ENTITY_TYPE_SCHEMA)
            .setParentEntityId(schemaId.parentId())
            .setEntityId(schemaId.entityId())
            .build());
    }

    private MetaServiceClient getSubMetaService(Meta.DingoCommonId schemaId) {
        // TODO return metaServiceCache.get(schemaId);
        Meta.GetSchemaRequest request = Meta.GetSchemaRequest.newBuilder().setSchemaId(schemaId).build();

        Meta.GetSchemaResponse response = metaConnector.exec(stub -> {
            Meta.GetSchemaResponse res = stub.getSchema(request);
            return new ServiceConnector.Response<>(res.getError(), res);
        }).getResponse();
        Meta.Schema schema = response.getSchema();
        return new MetaServiceClient(schema.getId(), schema.getName(), metaConnector);
    }

    /* TODO
    public boolean dropSubMetaService(String name) {
        return Optional.ofNullable(metaServiceIdCache.get(name))
                .map(schemaId -> {
                    Meta.DropSchemaRequest request = Meta.DropSchemaRequest.newBuilder()
                            .setSchemaId(metaServiceIdCache.get(name))
                            .build();
                    Meta.DropSchemaResponse response = metaConnector.exec(stub -> {
                        Meta.DropSchemaResponse res = stub.dropSchema(request);
                        return new ServiceConnector.Response<>(res.getError(), res);
                    }).getResponse();
                    return response.getError().getErrcodeValue() == 0;
                })
                .orElse(false);
    }*/

    public boolean dropSubMetaService(DingoCommonId schemaId) {
        Meta.DropSchemaRequest request = Meta.DropSchemaRequest.newBuilder()
                .setSchemaId(mapping(schemaId))
                .build();

        Meta.DropSchemaResponse response = metaConnector.exec(stub -> {
            Meta.DropSchemaResponse res = stub.dropSchema(request);
            return new ServiceConnector.Response<>(res.getError(), res);
        }).getResponse();

        return response.getError().getErrcodeValue() == 0;
    }

    private void addTableCache(Meta.TableDefinitionWithId tableDefinitionWithId) {
        Meta.DingoCommonId tableId = tableDefinitionWithId.getTableId();
        String name = tableDefinitionWithId.getTableDefinition().getName();
        tableIdCache.computeIfAbsent(name, __ -> tableId);
        tableDefinitionCache.computeIfAbsent(mapping(tableId), __ -> mapping(tableDefinitionWithId));
    }

    public boolean createTable(@NonNull String tableName, @NonNull Table table) {
        tableName = cleanTableName(tableName);
        if (Optional.mapOrNull(getTableId(tableName), this::getTableDefinition) != null) {
            throw new DingoClientException("Table " + tableName + " already exists");
        }

        Meta.CreateTableIdRequest createTableIdRequest = Meta.CreateTableIdRequest.newBuilder()
                .setSchemaId(id)
                .build();

        Meta.CreateTableIdResponse createTableIdResponse = metaConnector.exec(stub -> {
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

        Meta.CreateTableResponse response = metaConnector.exec(stub -> {
            Meta.CreateTableResponse res = stub.createTable(request);
            return new ServiceConnector.Response<>(res.getError(), res);
        }).getResponse();


        /* TODO
        tableIdCache.put(tableName, tableId);
        tableDefinitionCache.put(mapping(tableId), table);*/

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

        Meta.DropTableResponse response = metaConnector.exec(stub -> {
            Meta.DropTableResponse res = stub.dropTable(request);
            return new ServiceConnector.Response<>(res.getError(), res);
        }).getResponse();
        /* TODO
        tableIdCache.remove(tableName);
        tableDefinitionCache.remove(tableId);*/

        return response.getError().getErrcodeValue() == 0;
    }

    public DingoCommonId getTableId(@NonNull String tableName) {
        tableName = cleanTableName(tableName);
        /* TODO
        Meta.DingoCommonId tableId = tableIdCache.get(tableName);
        if (tableId == null) {
            Meta.GetTablesRequest request = Meta.GetTablesRequest.newBuilder()
                    .setSchemaId(id)
                    .build();

            Meta.GetTablesResponse response = metaConnector.exec(stub -> {
                Meta.GetTablesResponse res = stub.getTables(request);
                return new ServiceConnector.Response<>(res.getError(), res);
            }).getResponse();

            for (Meta.TableDefinitionWithId td : response.getTableDefinitionWithIdsList()) {
                if (tableName.equals(td.getTableDefinition().getName())) {
                    addTableCache(td);
                    break;
                }
            }
        }*/
        Meta.GetTableByNameRequest request = Meta.GetTableByNameRequest.newBuilder()
                .setSchemaId(id)
                .setTableName(tableName)
                .build();

        Meta.GetTableByNameResponse response = metaConnector.exec(stub -> {
            Meta.GetTableByNameResponse res = stub.getTableByName(request);
            return new ServiceConnector.Response<>(res.getError(), res);
        }).getResponse();

        Meta.TableDefinitionWithId withId = response.getTableDefinitionWithId();
        if (withId.getTableDefinition().getName().equals(tableName)) {
            return Optional.mapOrNull(withId.getTableId(), EntityConversion::mapping);
        }
        return null;
    }

    public Map<String, Table> getTableDefinitions() {
        if (!(id == ROOT_SCHEMA_ID)) {
            List<Meta.TableDefinitionWithId> tableDefinitions = getTableDefinitions(id);
            return tableDefinitions.stream()
                    .map(EntityConversion::mapping)
                    .collect(Collectors.toMap(Table::getName, Function.identity()));
        }
        return Collections.emptyMap();
        /* TODO
        if (tableDefinitionCache.isEmpty()) {
            reload();
        }
        return tableDefinitionCache.values().stream()
                .collect(Collectors.toMap(Table::getName, Function.identity()));*/
    }

    private List<Meta.TableDefinitionWithId> getTableDefinitions(Meta.DingoCommonId id) {
        Meta.GetTablesRequest request = Meta.GetTablesRequest.newBuilder()
                .setSchemaId(id)
                .build();

        Meta.GetTablesResponse response = metaConnector.exec(stub -> {
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
        /* TODO
        Table table = tableDefinitionCache.get(tableId);
        if (table == null) {
            Meta.GetTableRequest request = Meta.GetTableRequest.newBuilder().setTableId(mapping(tableId)).build();
            Meta.GetTableResponse response = metaConnector.exec(stub -> {
                Meta.GetTableResponse res = stub.getTable(request);
                return new ServiceConnector.Response<>(res.getError(), res);
            }).getResponse();
            table = mapping(response.getTableDefinitionWithId());
        }
        return table;
        }*/
        Meta.GetTableRequest request = Meta.GetTableRequest.newBuilder().setTableId(mapping(tableId)).build();
        Meta.GetTableResponse response = metaConnector.exec(stub -> {
            Meta.GetTableResponse res = stub.getTable(request);
            return new ServiceConnector.Response<>(res.getError(), res);
        }).getResponse();
        return mapping(response.getTableDefinitionWithId());
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

        Meta.GetTableRangeResponse response = metaConnector.exec(stub -> {
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
                    Meta.GetTableMetricsResponse response = metaConnector.exec(stub -> {
                        Meta.GetTableMetricsResponse res = stub.getTableMetrics(request);
                        return new ServiceConnector.Response<>(res.getError(), res);
                    }).getResponse();
                    return response.getTableMetrics().getTableMetrics();
                })
                .mapOrNull(EntityConversion::mapping);
        /* TODO
        return tableMetricsCache.computeIfAbsent(tableId, ___ -> Optional.ofNullable(tableId)
                .map(__ -> {
                    Meta.GetTableMetricsRequest request = Meta.GetTableMetricsRequest.newBuilder()
                            .setTableId(mapping(__))
                            .build();
                    Meta.GetTableMetricsResponse response = metaConnector.exec(stub -> {
                        Meta.GetTableMetricsResponse res = stub.getTableMetrics(request);
                        return new ServiceConnector.Response<>(res.getError(), res);
                    }).getResponse();
                    return response.getTableMetrics().getTableMetrics();
                })
                .mapOrNull(EntityConversion::mapping));*/
    }

    @Deprecated
    public void generateAutoIncrement(DingoCommonId tableId, Long count, Integer increment, Integer offset) {
        throw new UnsupportedOperationException("Using increment service.");
    }

    @Deprecated
    private void removeAutoIncrementCache(DingoCommonId tableId) {
        throw new UnsupportedOperationException("Using increment service.");
    }

    @Deprecated
    public synchronized Long getIncrementId(DingoCommonId tableId) {
        throw new UnsupportedOperationException("Using increment service.");
    }

    @Deprecated
    public Long getAutoIncrement(String tableName) {
        throw new UnsupportedOperationException("Using increment service.");
    }

    @Deprecated
    public Long getAutoIncrement(DingoCommonId tableId) {
        throw new UnsupportedOperationException("Using increment service.");
    }

    private String cleanTableName(String name) {
        return cleanName(name, "Table");
    }

    private String cleanColumnName(String name) {
        return cleanName(name, "Column");
    }

    private String cleanName(String name, String source) {
        if (warnPattern.matcher(name).matches()) {
            log.warn("{} name currently only supports uppercase letters, LowerCase -> UpperCase", source);
            name = name.toUpperCase();
        }
        if (!pattern.matcher(name).matches()) {
            throw new DingoClientException(source + " name currently only supports uppercase letters, digits, and underscores");
        }
        return name;
    }
}
