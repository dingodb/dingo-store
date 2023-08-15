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

import com.google.protobuf.ByteString;
import io.dingodb.coordinator.Coordinator;
import io.dingodb.meta.Meta;
import io.dingodb.meta.MetaServiceGrpc;
import io.dingodb.sdk.common.DingoClientException;
import io.dingodb.sdk.common.DingoCommonId;
import io.dingodb.sdk.common.codec.DingoKeyValueCodec;
import io.dingodb.sdk.common.index.Index;
import io.dingodb.sdk.common.index.IndexMetrics;
import io.dingodb.sdk.common.partition.Partition;
import io.dingodb.sdk.common.partition.PartitionDetail;
import io.dingodb.sdk.common.table.RangeDistribution;
import io.dingodb.sdk.common.table.Table;
import io.dingodb.sdk.common.table.metric.TableMetrics;
import io.dingodb.sdk.common.utils.ByteArrayUtils.ComparableByteArray;
import io.dingodb.sdk.common.utils.EntityConversion;
import io.dingodb.sdk.common.utils.Optional;
import io.dingodb.sdk.common.utils.Parameters;
import io.dingodb.sdk.service.connector.MetaServiceConnector;
import io.dingodb.sdk.service.connector.ServiceConnector;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static io.dingodb.sdk.common.utils.ByteArrayUtils.POS;
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

    private MetaServiceConnector metaConnector;

    public MetaServiceClient(String servers) {
        this.parentId = ROOT_SCHEMA_ID;
        this.id = ROOT_SCHEMA_ID;
        this.name = ROOT_NAME;
        this.metaConnector = MetaServiceConnector.getMetaServiceConnector(servers);
        // TODO reloadExecutor.execute(this::reload);
    }

    private MetaServiceClient(
            Meta.DingoCommonId id,
            String name,
            MetaServiceConnector metaConnector) {
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
            this.getTableDefinitionsBySchema(id).forEach(this::addTableCache);
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
        Meta.CreateSchemaResponse response = metaConnector.exec(stub -> stub.createSchema(request));

        // TODO addMetaServiceCache(schema);
    }

    public List<Meta.Schema> getSchemas(Meta.DingoCommonId id) {
        Meta.GetSchemasRequest request = Meta.GetSchemasRequest.newBuilder()
                .setSchemaId(id)
                .build();

        return Optional.mapOrGet(
            metaConnector.exec(stub -> stub.getSchemas(request)),
            Meta.GetSchemasResponse::getSchemasList,
            Collections::emptyList
        );
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

        return Optional
            .ofNullable(metaConnector.exec(stub -> stub.getSchemaByName(request)))
            .map(Meta.GetSchemaByNameResponse::getSchema)
            .mapOrNull(__ -> new MetaServiceClient(__.getId(), __.getName(), metaConnector));
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

        Meta.GetSchemaResponse response = metaConnector.exec(stub -> stub.getSchema(request));
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

        return metaConnector.exec(stub -> stub.dropSchema(request)) != null;
    }

    private void addTableCache(Meta.TableDefinitionWithId tableDefinitionWithId) {
        Meta.DingoCommonId tableId = tableDefinitionWithId.getTableId();
        String name = tableDefinitionWithId.getTableDefinition().getName();
        tableIdCache.computeIfAbsent(name, __ -> tableId);
        tableDefinitionCache.computeIfAbsent(mapping(tableId), __ -> mapping(tableDefinitionWithId));
    }

    public boolean createTable(@NonNull String tableName, @NonNull Table table) {
        return createTables(table, new ArrayList<>());
    }

    public boolean createTables(@NonNull Table table, List<Table> indexes) {
        String tableName = cleanTableName(table.getName());
        if (Optional.mapOrNull(getTableId(tableName), this::getTableDefinition) != null) {
            throw new DingoClientException("Table " + tableName + " already exists");
        }

        List<Integer> partCount = indexes.stream()
            .map(i -> Optional.of(i.getPartition()).map(Partition::getDetails).map(List::size).orElse(0) + 1)
            .collect(Collectors.toList());
        Meta.GenerateTableIdsRequest request = Meta.GenerateTableIdsRequest.newBuilder()
                .setSchemaId(id)
                .setCount(Meta.TableWithPartCount.newBuilder()
                        .setHasTable(true)
                        .setTablePartCount(Optional.mapOrGet(table.getPartition(), __ -> __.getDetails().size() + 1, () -> 1))
                        .setIndexCount(indexes.size())
                        .addAllIndexPartCount(partCount)
                        .build())
                .build();

        List<Meta.TableIdWithPartIds> ids = metaConnector.exec(stub -> stub.generateTableIds(request)).getIdsList();

        Meta.TableIdWithPartIds tableWithId = ids.stream()
                .filter(i -> i.getTableId().getEntityType().name().equals(Meta.EntityType.ENTITY_TYPE_TABLE.name()))
                .findAny().get();
        List<Meta.TableIdWithPartIds> indexIds = ids.stream()
                .filter(i -> i.getTableId().getEntityType().name().equals(Meta.EntityType.ENTITY_TYPE_INDEX.name()))
                .collect(Collectors.toList());

        Meta.CreateTablesRequest.Builder builder = Meta.CreateTablesRequest.newBuilder().setSchemaId(id);
        if (indexIds.size() == partCount.size()) {
            for (int i = 0; i < indexIds.size(); i++) {
                Table index = indexes.get(i);
                Meta.TableIdWithPartIds tableIdWithPartId = indexIds.get(i);
                Meta.TableDefinition indexDefinition = mapping(index, tableIdWithPartId.getTableId(), tableIdWithPartId.getPartIdsList());
                builder.addTableDefinitionWithIds(Meta.TableDefinitionWithId.newBuilder()
                        .setTableId(tableIdWithPartId.getTableId())
                        .setTableDefinition(indexDefinition)
                        .build());
            }
        }
        builder.addTableDefinitionWithIds(Meta.TableDefinitionWithId.newBuilder()
                .setTableId(tableWithId.getTableId())
                .setTableDefinition(mapping(table, tableWithId.getTableId(), tableWithId.getPartIdsList()))
                .build());

        Meta.CreateTablesResponse response = metaConnector.exec(stub -> stub.createTables(builder.build()));

        return response != null;
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

        Meta.DropTableResponse response = metaConnector.exec(stub -> stub.dropTable(request));
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
            Meta.GetTablesBySchemaRequest request = Meta.GetTablesBySchemaRequest.newBuilder()
                    .setSchemaId(id)
                    .build();

            Meta.GetTablesBySchemaResponse response = metaConnector.exec(stub -> {
                Meta.GetTablesBySchemaResponse res = stub.getTables(request);
                return new ServiceConnector.Response<>(res.getError(), res);
            }).getResponse();

            for (Meta.TableDefinitionWithId td : response.getTableDefinitionWithIdsList()) {
                if (tableName.equals(td.getTableDefinition().getName())) {
                    addTableCache(td);
                    break;
                }
            }
        }*/
        return Optional.mapOrNull(getTableDefinitionWithId(tableName), __ -> EntityConversion.mapping(__.getTableId()));
    }

    /**
     * Use {@link MetaServiceClient#getTableDefinition} and {@link MetaServiceClient#getTableIndexes}
     * Get assigned table definition, including vector index and scalar index.
     * @param tableName table name
     * @return table definition/vector index/scalar index
     */
    @Deprecated
    public List<Table> getTables(String tableName) {
        tableName = cleanTableName(tableName);
        DingoCommonId tableId = getTableId(tableName);
        if (tableId == null) {
            throw new DingoClientException("Table " + tableName + " does not exist");
        }

        Meta.GetTablesRequest request = Meta.GetTablesRequest.newBuilder().setTableId(mapping(tableId)).build();

        Meta.GetTablesResponse response = metaConnector.exec(stub -> stub.getTables(request));

        return response.getTableDefinitionWithIdsList().stream()
                .map(EntityConversion::mapping)
                .collect(Collectors.toList());
    }

    public Map<DingoCommonId, Table> getTableIndexes(String tableName) {
        tableName = cleanTableName(tableName);
        DingoCommonId tableId = getTableId(tableName);
        if (tableId == null) {
            throw new DingoClientException("Table " + tableName + " does not exist");
        }

        return getTableIndexes(tableId);
    }

    public Map<DingoCommonId, Table> getTableIndexes(DingoCommonId tableId) {
        Meta.DingoCommonId metaTableId = mapping(tableId);
        Meta.GetTablesRequest request = Meta.GetTablesRequest.newBuilder().setTableId(metaTableId).build();

        Meta.GetTablesResponse response = metaConnector.exec(stub -> stub.getTables(request));

        return response.getTableDefinitionWithIdsList().stream()
            .filter(__ -> !__.getTableId().equals(metaTableId))
            .collect(Collectors.toMap(__ -> mapping(__.getTableId()), EntityConversion::mapping));
    }


    public Map<String, Table> getTableDefinitionsBySchema() {
        if (!(id == ROOT_SCHEMA_ID)) {
            List<Meta.TableDefinitionWithId> tableDefinitions = getTableDefinitionsBySchema(id);
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

    private List<Meta.TableDefinitionWithId> getTableDefinitionsBySchema(Meta.DingoCommonId id) {
        Meta.GetTablesBySchemaRequest request = Meta.GetTablesBySchemaRequest.newBuilder()
                .setSchemaId(id)
                .build();

        return metaConnector.exec(stub -> stub.getTablesBySchema(request)).getTableDefinitionWithIdsList();
    }

    public Table getTableDefinition(@NonNull String tableName) {
        return Optional.mapOrThrow(
            getTableDefinitionWithId(cleanTableName(tableName)),
            EntityConversion::mapping,
            () -> new DingoClientException("Table " + tableName + " does not exist")
        );
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
        return mapping(metaConnector.exec(stub -> stub.getTable(request)).getTableDefinitionWithId());
    }

    private Meta.TableDefinitionWithId getTableDefinitionWithId(String tableName) {
        Meta.GetTableByNameRequest request = Meta.GetTableByNameRequest.newBuilder()
            .setSchemaId(id)
            .setTableName(tableName)
            .build();

        return Optional.ofNullable(metaConnector.exec(stub -> stub.getTableByName(request)))
            .map(Meta.GetTableByNameResponse::getTableDefinitionWithId)
            .filter(__ -> __.getTableDefinition().getName().equalsIgnoreCase(tableName))
            .orNull();
    }


    public void addDistribution(String tableName, PartitionDetail partitionDetail) {
        tableName = cleanTableName(tableName);
        Meta.TableDefinitionWithId definitionWithId = Parameters.nonNull(
            getTableDefinitionWithId(tableName), "Table " + tableName + " dose not exist"
        );
        Table table = mapping(definitionWithId);
        DingoCommonId tableId = mapping(definitionWithId.getTableId());
        DingoKeyValueCodec codec = DingoKeyValueCodec.of(tableId.entityId(), table.getKeyColumns());
        try {
            byte[] key = codec.encodeKeyPrefix(partitionDetail.getOperand(), partitionDetail.getOperand().length);
            Coordinator.SplitRegionRequest request = Coordinator.SplitRegionRequest.newBuilder()
                .setSplitRequest(Coordinator.SplitRequest.newBuilder()
                    .setSplitFromRegionId(
                        getRangeDistribution(tableName, new ComparableByteArray(key, POS)).getId().entityId())
                    .setSplitWatershedKey(ByteString.copyFrom(key))
                    .build())
                .build();
            metaConnector.getCoordinatorServiceConnector().exec(stub -> stub.splitRegion(request));
        } catch (Exception e) {
            throw new DingoClientException(-1, e);
        }
    }

    public RangeDistribution getRangeDistribution(String tableName, ComparableByteArray key) {
        return getRangeDistribution(cleanTableName(tableName)).floorEntry(key).getValue();
    }

    public RangeDistribution getRangeDistribution(String tableName, DingoCommonId regionId) {
        return getRangeDistribution(cleanTableName(tableName)).values().stream().filter(r -> r.getId().equals(regionId))
            .findAny().orElseThrow(() -> new DingoClientException("Not found region " + tableName + ":" + regionId));
    }

    public RangeDistribution getRangeDistribution(DingoCommonId id, ComparableByteArray key) {
        return getRangeDistribution(id).floorEntry(key).getValue();
    }

    public RangeDistribution getRangeDistribution(DingoCommonId id, DingoCommonId regionId) {
        return getRangeDistribution(id).values().stream().filter(r -> r.getId().equals(regionId))
            .findAny().orElseThrow(() -> new DingoClientException("Not found region " + id + ":" + regionId));
    }

    public NavigableMap<ComparableByteArray, RangeDistribution> getRangeDistribution(String tableName) {
        tableName = cleanTableName(tableName);
        DingoCommonId tableId = getTableId(tableName);
        if (tableId == null) {
            throw new DingoClientException("Table " + tableName + " does not exist");
        }
        return getRangeDistribution(tableId);
    }

    public NavigableMap<ComparableByteArray, RangeDistribution> getRangeDistribution(DingoCommonId id) {
        NavigableMap<ComparableByteArray, RangeDistribution> result = new TreeMap<>();
        Meta.GetTableRangeRequest request = Meta.GetTableRangeRequest.newBuilder()
                .setTableId(mapping(id))
                .build();

        Meta.GetTableRangeResponse response = metaConnector.exec(stub -> stub.getTableRange(request));

        for (Meta.RangeDistribution tablePart : response.getTableRange().getRangeDistributionList()) {
            result.put(new ComparableByteArray(
                            tablePart.getRange().getStartKey().toByteArray(), POS),
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
                    return Optional
                            .ofNullable(metaConnector.exec(stub -> stub.getTableMetrics(request)))
                            .map(Meta.GetTableMetricsResponse::getTableMetrics)
                            .map(Meta.TableMetricsWithId::getTableMetrics)
                            .orNull();
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

    public boolean createIndex(String name, Index index) {
        Meta.GenerateTableIdsRequest generateRequest = Meta.GenerateTableIdsRequest.newBuilder()
                .setSchemaId(id)
                .setCount(Meta.TableWithPartCount.newBuilder()
                        .setHasTable(false)
                        .setIndexCount(1)
                        .addAllIndexPartCount(Collections.singletonList(index.getIndexPartition().getDetails().size() + 1))
                        .build())
                .build();

        Meta.GenerateTableIdsResponse generateResponse = metaConnector.exec(stub -> stub.generateTableIds(generateRequest));

        if (generateResponse.getIdsCount() <= 0) {
            throw new DingoClientException("Index id generation failed");
        }
        Meta.TableIdWithPartIds withPartIds = generateResponse.getIdsList().get(0);
        Meta.CreateIndexRequest request = Meta.CreateIndexRequest.newBuilder()
                .setSchemaId(id)
                .setIndexId(withPartIds.getTableId())
                .setIndexDefinition(mapping(withPartIds.getTableId().getEntityId(), index, withPartIds.getPartIdsList()))
                .build();

        Meta.CreateIndexResponse response = metaConnector.exec(stub -> stub.createIndex(request));

        return response != null;
    }

    public boolean updateIndex(String index, Index newIndex) {
        // ignore table index
        if (index.contains(".")) {
            return false;
        }
        DingoCommonId indexId = getIndexId(index);
        Meta.UpdateIndexRequest request = Meta.UpdateIndexRequest.newBuilder()
                .setIndexId(mapping(indexId))
                .setNewIndexDefinition(mapping(indexId.entityId(), newIndex, Collections.emptyList()))
                .build();

        Meta.UpdateIndexResponse response = metaConnector.exec(stub -> stub.updateIndex(request));

        return response != null;
    }

    public boolean dropIndex(String indexName) {
        // ignore table index
        if (indexName.contains(".")) {
            return false;
        }
        DingoCommonId indexId = getIndexId(indexName);
        return dropIndex(indexId);
    }

    public boolean dropIndex(DingoCommonId indexId) {
        Meta.DropIndexRequest request = Meta.DropIndexRequest.newBuilder().setIndexId(mapping(indexId)).build();

        Meta.DropIndexResponse response = metaConnector.exec(stub -> stub.dropIndex(request));

        return response.getError().getErrcodeValue() == 0;
    }

    public Index getIndex(DingoCommonId indexId) {
        Meta.GetIndexRequest request = Meta.GetIndexRequest.newBuilder().setIndexId(mapping(indexId)).build();

        Meta.GetIndexResponse response = metaConnector.exec(stub -> stub.getIndex(request));

        // ignore table index
        if (response.getIndexDefinitionWithId().getIndexDefinition().getName().contains(".")) {
            return null;
        }

        return mapping(indexId.entityId(), response.getIndexDefinitionWithId().getIndexDefinition());
    }

    public Index getIndex(String name) {
        // ignore table index
        if (name.contains(".")) {
            return null;
        }
        Meta.GetIndexByNameRequest request = Meta.GetIndexByNameRequest.newBuilder()
                .setSchemaId(id)
                .setIndexName(name)
                .build();

        Meta.GetIndexByNameResponse response = metaConnector.exec(stub -> stub.getIndexByName(request));
        Meta.IndexDefinitionWithId withId = response.getIndexDefinitionWithId();
        return mapping(withId.getIndexId().getEntityId(), withId.getIndexDefinition());
    }

    public Map<DingoCommonId, Index> getIndexes(DingoCommonId schemaId) {
        Map<DingoCommonId, Index> results = new ConcurrentHashMap<>();
        Meta.GetIndexesRequest request = Meta.GetIndexesRequest.newBuilder()
                .setSchemaId(mapping(schemaId))
                .build();

        Meta.GetIndexesResponse response = metaConnector.exec(stub -> stub.getIndexes(request));

        for (Meta.IndexDefinitionWithId withId : response.getIndexDefinitionWithIdsList()) {
            // ignore table index
            if (withId.getIndexDefinition().getName().contains(".")) {
                continue;
            }
            results.put(mapping(withId.getIndexId()), mapping(withId.getIndexId().getEntityId(), withId.getIndexDefinition()));
        }

        return results;
    }

    public DingoCommonId getIndexId(String indexName) {
        return Optional.mapOrNull(getIndexDefinitionWithId(indexName), __ -> EntityConversion.mapping(__.getIndexId()));
    }

    private Meta.IndexDefinitionWithId getIndexDefinitionWithId(String indexName) {
        // ignore table index
        if (indexName.contains(".")) {
            return null;
        }
        Meta.GetIndexByNameRequest request = Meta.GetIndexByNameRequest.newBuilder()
                .setSchemaId(id)
                .setIndexName(indexName)
                .build();

        return Optional.ofNullable(metaConnector.exec(stub -> stub.getIndexByName(request)))
                .map(Meta.GetIndexByNameResponse::getIndexDefinitionWithId)
                .filter(__ -> __.getIndexDefinition().getName().equalsIgnoreCase(indexName))
                .orNull();
    }

    public NavigableMap<ComparableByteArray, RangeDistribution> getIndexRangeDistribution(String indexName) {
        DingoCommonId indexId = getIndexId(indexName);
        if (indexId == null) {
            throw new DingoClientException("Index " + indexName + " does not exist");
        }
        return getIndexRangeDistribution(indexId);
    }

    public NavigableMap<ComparableByteArray, RangeDistribution> getIndexRangeDistribution(DingoCommonId indexId) {
        NavigableMap<ComparableByteArray, RangeDistribution> result = new TreeMap<>();
        Meta.GetIndexRangeRequest request = Meta.GetIndexRangeRequest.newBuilder()
                .setIndexId(mapping(indexId))
                .build();

        Meta.GetIndexRangeResponse response = metaConnector.exec(stub -> stub.getIndexRange(request));

        for (Meta.RangeDistribution indexPart : response.getIndexRange().getRangeDistributionList()) {
            result.put(new ComparableByteArray(
                        indexPart.getRange().getStartKey().toByteArray(), POS),
                    mapping(indexPart));
        }
        return result;
    }

    public IndexMetrics getIndexMetrics(String index) {
        DingoCommonId indexId = getIndexId(index);
        if (indexId == null) {
            throw new DingoClientException("Index" + index + " does not exist");
        }

        return Optional.ofNullable(indexId)
                .map(__ -> {
                    Meta.GetIndexMetricsRequest request = Meta.GetIndexMetricsRequest.newBuilder()
                            .setIndexId(mapping(__))
                            .build();
                    return Optional.ofNullable(metaConnector.exec(stub -> stub.getIndexMetrics(request)))
                            .map(Meta.GetIndexMetricsResponse::getIndexMetrics)
                            .map(Meta.IndexMetricsWithId::getIndexMetrics)
                            .orNull();
                })
                .mapOrNull(EntityConversion::mapping);
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
