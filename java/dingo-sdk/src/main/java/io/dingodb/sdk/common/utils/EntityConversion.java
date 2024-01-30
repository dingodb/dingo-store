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

package io.dingodb.sdk.common.utils;

import com.google.common.collect.Maps;
import com.google.protobuf.ByteString;
import io.dingodb.common.Common;
import io.dingodb.meta.Meta;
import io.dingodb.sdk.common.Context;
import io.dingodb.sdk.common.DingoClientException;
import io.dingodb.sdk.common.DingoCommonId;
import io.dingodb.sdk.common.KeyValue;
import io.dingodb.sdk.common.Location;
import io.dingodb.sdk.common.Range;
import io.dingodb.sdk.common.RangeWithOptions;
import io.dingodb.sdk.common.SDKCommonId;
import io.dingodb.sdk.common.cluster.Executor;
import io.dingodb.sdk.common.cluster.ExecutorMap;
import io.dingodb.sdk.common.cluster.ExecutorUser;
import io.dingodb.sdk.common.cluster.InternalCoordinator;
import io.dingodb.sdk.common.cluster.InternalExecutor;
import io.dingodb.sdk.common.cluster.InternalExecutorMap;
import io.dingodb.sdk.common.cluster.InternalExecutorUser;
import io.dingodb.sdk.common.cluster.InternalRegion;
import io.dingodb.sdk.common.cluster.InternalStore;
import io.dingodb.sdk.common.codec.CodecUtils;
import io.dingodb.sdk.common.codec.DingoKeyValueCodec;
import io.dingodb.sdk.common.codec.KeyValueCodec;
import io.dingodb.sdk.common.index.BruteForceParam;
import io.dingodb.sdk.common.index.DiskAnnParam;
import io.dingodb.sdk.common.index.FlatParam;
import io.dingodb.sdk.common.index.HnswParam;
import io.dingodb.sdk.common.index.Index;
import io.dingodb.sdk.common.index.IndexDefinition;
import io.dingodb.sdk.common.index.IndexMetrics;
import io.dingodb.sdk.common.index.IndexParameter;
import io.dingodb.sdk.common.index.IvfFlatParam;
import io.dingodb.sdk.common.index.IvfPqParam;
import io.dingodb.sdk.common.index.ScalarIndexParameter;
import io.dingodb.sdk.common.index.VectorIndexParameter;
import io.dingodb.sdk.common.partition.Partition;
import io.dingodb.sdk.common.partition.PartitionDetail;
import io.dingodb.sdk.common.partition.PartitionDetailDefinition;
import io.dingodb.sdk.common.partition.PartitionRule;
import io.dingodb.sdk.common.region.RegionEpoch;
import io.dingodb.sdk.common.region.RegionHeartbeatState;
import io.dingodb.sdk.common.region.RegionState;
import io.dingodb.sdk.common.region.RegionStatus;
import io.dingodb.sdk.common.serial.schema.DingoSchema;
import io.dingodb.sdk.common.serial.schema.LongSchema;
import io.dingodb.sdk.common.serial.schema.Type;
import io.dingodb.sdk.common.table.Column;
import io.dingodb.sdk.common.table.ColumnDefinition;
import io.dingodb.sdk.common.table.RangeDistribution;
import io.dingodb.sdk.common.table.Table;
import io.dingodb.sdk.common.table.TableDefinition;
import io.dingodb.sdk.common.table.metric.TableMetrics;
import io.dingodb.sdk.common.vector.ScalarField;
import io.dingodb.sdk.common.vector.ScalarValue;
import io.dingodb.sdk.common.vector.Vector;
import io.dingodb.sdk.common.vector.VectorIndexMetrics;
import io.dingodb.sdk.common.vector.VectorTableData;
import io.dingodb.sdk.common.vector.VectorWithDistance;
import io.dingodb.sdk.common.vector.VectorWithId;
import io.dingodb.sdk.service.store.AggregationOperator;
import io.dingodb.sdk.service.store.Coprocessor;
import io.dingodb.store.Store;

import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.dingodb.common.Common.IndexType.INDEX_TYPE_SCALAR;
import static io.dingodb.common.Common.IndexType.INDEX_TYPE_VECTOR;
import static io.dingodb.sdk.common.utils.NoBreakFunctions.wrap;
import static io.dingodb.sdk.common.utils.Parameters.cleanNull;

public class EntityConversion {

    public static final LongSchema SCHEMA = new LongSchema(0);

    public static Meta.TableDefinition mapping(Table table, Meta.DingoCommonId tableId, List<Meta.DingoCommonId> partitionIds) {
        Optional.ofNullable(table.getColumns())
                .filter(__ -> __.stream()
                        .map(Column::getName)
                        .distinct()
                        .count() == __.size())
                .orElseThrow(() -> new DingoClientException("Table field names cannot be repeated."));
        List<Meta.ColumnDefinition> columnDefinitions = table.getColumns().stream()
                .map(EntityConversion::mapping)
                .collect(Collectors.toList());
        return Meta.TableDefinition.newBuilder()
                .setName(table.getName().toUpperCase())
                .setVersion(table.getVersion())
                .setTtl(table.getTtl())
                .setTablePartition(calcRange(table, tableId, partitionIds))
                .setEngine(Common.Engine.valueOf(table.getEngine()))
                .setReplica(table.getReplica())
                .addAllColumns(columnDefinitions)
                .setAutoIncrement(table.getAutoIncrement())
                .putAllProperties(table.getProperties() == null ? new HashMap() : table.getProperties())
                .setCreateSql(Parameters.cleanNull(table.getCreateSql(), ""))
                .setIndexParameter(Optional.mapOrGet(table.getIndexParameter(), EntityConversion::mapping, () -> Common.IndexParameter.newBuilder().build()))
                .setComment(Parameters.cleanNull(table.getComment(), ""))
                .setCharset(Parameters.cleanNull(table.getCharset(), "utf8"))
                .setCollate(Parameters.cleanNull(table.getCollate(), "utf8_bin"))
                .setTableType(Parameters.cleanNull(table.getTableType(), "BASE TABLE"))
                .setRowFormat(Parameters.cleanNull(table.getRowFormat(), "Dynamic"))
                .build();
    }

    public static Table mapping(Meta.TableDefinitionWithId tableDefinitionWithId) {
        Meta.TableDefinition tableDefinition = tableDefinitionWithId.getTableDefinition();
        List<Column> columns = tableDefinition.getColumnsList().stream()
                .map(EntityConversion::mapping)
                .collect(Collectors.toList());
        return TableDefinition.builder()
                .name(tableDefinition.getName())
                .columns(columns)
                .version(tableDefinition.getVersion())
                .ttl((int) tableDefinition.getTtl())
                .engine(tableDefinition.getEngine().name())
                .properties(tableDefinition.getPropertiesMap())
                .partition(mapping(tableDefinitionWithId.getTableId().getEntityId(), tableDefinition, columns))
                .replica(tableDefinition.getReplica())
                .autoIncrement(tableDefinition.getAutoIncrement())
                .createSql(tableDefinition.getCreateSql())
                .indexParameter(Optional.mapOrNull(tableDefinition.getIndexParameter(), EntityConversion::mapping))
                .comment(tableDefinition.getComment())
                .charset(Parameters.cleanNull(tableDefinition.getCharset(), "utf8"))
                .collate(Parameters.cleanNull(tableDefinition.getCollate(), "utf8_bin"))
                .tableType(Parameters.cleanNull(tableDefinition.getTableType(), "BASE TABLE"))
                .rowFormat(Parameters.cleanNull(tableDefinition.getRowFormat(), "Dynamic"))
                .createTime(tableDefinition.getCreateTimestamp())
                .updateTime(tableDefinition.getUpdateTimestamp())
                .build();
    }

    public static Partition mapping(long id, Meta.TableDefinition tableDefinition, List<Column> columns) {
        Meta.PartitionRule partition = tableDefinition.getTablePartition();
        if (partition.getPartitionsCount() < 1) {
            return null;
        }
        DingoKeyValueCodec codec = DingoKeyValueCodec.of(id, columns);
        List<PartitionDetail> details = partition.getPartitionsList().stream()
                .map(Meta.Partition::getRange)
                .map(Common.Range::getStartKey)
                .map(ByteString::toByteArray)
                .map(__ -> codec.resetPrefix(__, id))
                .sorted(ByteArrayUtils::compare)
                .skip(1)
                .map(wrap(codec::decodeKeyPrefix))
                .map(key -> new PartitionDetailDefinition(null, null, key))
                .collect(Collectors.toList());

        return new PartitionRule(getStrategy(partition.getStrategy()), partition.getColumnsList(), details);
    }

    public static Column mapping(Meta.ColumnDefinition definition) {
        return ColumnDefinition.builder()
                .name(definition.getName())
                .type(definition.getSqlType())
                .elementType(definition.getElementType())
                .precision(definition.getPrecision())
                .scale(definition.getScale())
                .nullable(definition.getNullable())
                .primary(definition.getIndexOfKey())
                .defaultValue(definition.getDefaultVal())
                .isAutoIncrement(definition.getIsAutoIncrement())
                .state(definition.getState() == 0 ? 1 : definition.getState())
                .comment(definition.getComment())
                .build();
    }

    public static TableMetrics mapping(Meta.TableMetrics metrics) {
        return new TableMetrics(
                metrics.getMinKey().toByteArray(),
                metrics.getMaxKey().toByteArray(),
                metrics.getRowsCount(),
                metrics.getPartCount());
    }

    public static KeyValue mapping(Common.KeyValue keyValue) {
        return new KeyValue(keyValue.getKey().toByteArray(), keyValue.getValue().toByteArray());
    }

    public static Common.KeyValue mapping(KeyValue keyValue) {
        return Common.KeyValue.newBuilder()
                .setKey(ByteString.copyFrom(keyValue.getKey()))
                .setValue(ByteString.copyFrom(keyValue.getValue()))
                .build();
    }

    public static Store.Context mapping(Context context) {
        return Store.Context.newBuilder()
                .setRegionId(context.getRegionId().entityId())
                .setRegionEpoch(mapping(context.getRegionEpoch()))
                .build();
    }

    public static RangeDistribution mapping(Meta.RangeDistribution rangeDistribution) {
        return new RangeDistribution(
                mapping(rangeDistribution.getId()),
                mapping(rangeDistribution.getRange()),
                mapping(rangeDistribution.getLeader()),
                rangeDistribution.getVotersList().stream().map(EntityConversion::mapping).collect(Collectors.toList()),
                mapping(rangeDistribution.getRegionEpoch()),
                mapping(rangeDistribution.getStatus())
        );
    }

    public static RegionEpoch mapping(Common.RegionEpoch regionEpoch) {
        return new RegionEpoch(regionEpoch.getConfVersion(), regionEpoch.getVersion());
    }

    public static Common.RegionEpoch mapping(RegionEpoch regionEpoch) {
        return Common.RegionEpoch.newBuilder()
                .setConfVersion(regionEpoch.getConfVersion())
                .setVersion(regionEpoch.getVersion())
                .build();
    }

    public static RegionStatus mapping(Meta.RegionStatus status) {
        return new RegionStatus(
                RegionState.valueOf(status.getState().name()),
                RegionHeartbeatState.valueOf(status.getHeartbeatState().name()));
    }

    public static Location mapping(Common.Location location) {
        return new Location(location.getHost(), location.getPort());
    }

    public static Common.Location mapping(Location location) {
        return Common.Location.newBuilder().setHost(location.getHost()).setPort(location.getPort()).build();
    }

    public static Range mapping(Common.Range range) {
        return new Range(range.getStartKey().toByteArray(), range.getEndKey().toByteArray());
    }

    public static Common.Range mapping(Range range) {
        return Common.Range.newBuilder()
                .setStartKey(ByteString.copyFrom(range.getStartKey()))
                .setEndKey(ByteString.copyFrom(range.getEndKey()))
                .build();
    }

    public static RangeWithOptions mapping(Common.RangeWithOptions options) {
        return new RangeWithOptions(
                mapping(options.getRange()),
                options.getWithStart(),
                options.getWithEnd());
    }

    public static Common.RangeWithOptions mapping(RangeWithOptions options) {
        return Common.RangeWithOptions.newBuilder()
                .setRange(mapping(options.getRange()))
                .setWithStart(options.isWithStart())
                .setWithEnd(options.isWithEnd())
                .build();
    }

    public static Meta.DingoCommonId mapping(DingoCommonId commonId) {
        return Meta.DingoCommonId.newBuilder()
                .setEntityType(Meta.EntityType.valueOf(commonId.type().name()))
                .setParentEntityId(commonId.parentId())
                .setEntityId(commonId.entityId())
                .build();
    }

    public static DingoCommonId mapping(Meta.DingoCommonId commonId) {
        return new SDKCommonId(
                DingoCommonId.Type.valueOf(commonId.getEntityType().name()),
                commonId.getParentEntityId(),
                commonId.getEntityId());
    }

    public static Common.Executor mapping(Executor executor) {
        return Common.Executor.newBuilder()
                .setServerLocation(mapping(executor.serverLocation()))
                .setExecutorUser(mapping(executor.executorUser()))
                .build();
    }

    public static Executor mapping(Common.Executor executor) {
        return new InternalExecutor(
                mapping(executor.getServerLocation()),
                mapping(executor.getExecutorUser()),
                executor.getResourceTag());
    }

    public static Common.ExecutorUser mapping(ExecutorUser executorUser) {
        return Common.ExecutorUser.newBuilder()
                .setUser(executorUser.getUser())
                .setKeyring(executorUser.getKeyring())
                .build();
    }

    public static ExecutorUser mapping(Common.ExecutorUser executorUser) {
        return new InternalExecutorUser(
                executorUser.getUser(),
                executorUser.getKeyring());
    }

    public static ExecutorMap mapping(Common.ExecutorMap executorMap) {
        return new InternalExecutorMap(
                executorMap.getEpoch(),
                executorMap.getExecutorsList()
                        .stream()
                        .map(EntityConversion::mapping)
                        .collect(Collectors.toList()));
    }

    public static Index mapping(long id, Meta.IndexDefinition definition) {
        return new IndexDefinition(
                definition.getName(),
                definition.getVersion(),
                mapping(id, definition.getIndexPartition()),
                definition.getReplica(),
                mapping(definition.getIndexParameter()),
                definition.getWithAutoIncrment(),
                definition.getAutoIncrement());
    }

    public static PartitionRule mapping(long id, Meta.PartitionRule partition) {
        if (partition.getPartitionsCount() < 1) {
            return null;
        }
        SCHEMA.setIsKey(true);
        SCHEMA.setAllowNull(false);
        DingoKeyValueCodec codec = new DingoKeyValueCodec(id, Collections.singletonList(SCHEMA));
        List<PartitionDetail> details = partition.getPartitionsList().stream()
                .map(Meta.Partition::getRange)
                .map(Common.Range::getStartKey)
                .map(ByteString::toByteArray)
                .map(__ -> codec.resetPrefix(__, id))
                .sorted(ByteArrayUtils::compare)
                .skip(1)
                .map(codec::decodeKeyPrefix)
                .map(key -> new PartitionDetailDefinition("", "", key))
                .collect(Collectors.toList());

        return new PartitionRule(getStrategy(partition.getStrategy()), partition.getColumnsList(), details);
    }

    private static String getStrategy(Meta.PartitionStrategy partitionStrategy) {
        String strategy;
        if (partitionStrategy == Meta.PartitionStrategy.PT_STRATEGY_HASH) {
            strategy = "HASH";
        } else {
            strategy = "RANGE";
        }
        return strategy;
    }

    private static void getStrategy(Partition partition, Meta.PartitionRule.Builder builder) {
        if (partition != null && partition.getFuncName() != null) {
            if (partition.getFuncName().equalsIgnoreCase("HASH")) {
                builder.setStrategy(Meta.PartitionStrategy.PT_STRATEGY_HASH);
            } else {
                builder.setStrategy(Meta.PartitionStrategy.PT_STRATEGY_RANGE);
            }
        }
    }

    public static Meta.IndexDefinition mapping(long id, Index index, List<Meta.DingoCommonId> partitionIds) {
        SCHEMA.setIsKey(true);
        SCHEMA.setAllowNull(false);
        DingoKeyValueCodec codec = new DingoKeyValueCodec(id, Collections.singletonList(SCHEMA));
        Iterator<byte[]> keys = Optional.<Partition, Stream<byte[]>>mapOrGet(
                index.getIndexPartition(), __ -> encodePartitionDetails(__.getDetails(), codec),
                                Stream::empty)
                .sorted(ByteArrayUtils::compare).iterator();

        Meta.PartitionRule.Builder builder = Meta.PartitionRule.newBuilder();
        byte[] start = codec.encodeMinKeyPrefix();
        for (Meta.DingoCommonId commonId : partitionIds) {
            builder.addPartitions(Meta.Partition.newBuilder()
                    .setId(commonId)
                    .setRange(Common.Range.newBuilder()
                            .setStartKey(ByteString.copyFrom(codec.resetPrefix(start, commonId.getEntityId())))
                            .setEndKey(ByteString.copyFrom(codec.resetPrefix(codec.encodeMaxKeyPrefix(), commonId.getEntityId() + 1)))
                            .build())
                    .build());
            start = keys.hasNext() ? keys.next() : start;
        }
        getStrategy(index.getIndexPartition(), builder);

        return Meta.IndexDefinition.newBuilder()
                .setName(index.getName())
                .setVersion(index.getVersion())
                .setIndexPartition(builder.build())
                .setReplica(index.getReplica())
                .setIndexParameter(mapping(index.getIndexParameter()))
                .setWithAutoIncrment(index.getIsAutoIncrement())
                .setAutoIncrement(index.getAutoIncrement())
                .build();
    }

    public static IndexParameter mapping(Common.IndexParameter parameter) {
        if (parameter.getIndexType() == INDEX_TYPE_VECTOR) {
            Common.VectorIndexParameter vectorParam = parameter.getVectorIndexParameter();
            switch (vectorParam.getVectorIndexType()) {
                case VECTOR_INDEX_TYPE_FLAT:
                    Common.CreateFlatParam flatParam = vectorParam.getFlatParameter();
                    return new IndexParameter(
                            IndexParameter.IndexType.valueOf(parameter.getIndexType().name()),
                            new VectorIndexParameter(VectorIndexParameter.VectorIndexType.VECTOR_INDEX_TYPE_FLAT,
                                    new FlatParam(
                                            flatParam.getDimension(),
                                            VectorIndexParameter.MetricType.valueOf(flatParam.getMetricType().name()))));
                case VECTOR_INDEX_TYPE_IVF_FLAT:
                    Common.CreateIvfFlatParam ivfFlatParam = vectorParam.getIvfFlatParameter();
                    return new IndexParameter(
                            IndexParameter.IndexType.valueOf(parameter.getIndexType().name()),
                            new VectorIndexParameter(VectorIndexParameter.VectorIndexType.VECTOR_INDEX_TYPE_IVF_FLAT,
                                    new IvfFlatParam(
                                            ivfFlatParam.getDimension(),
                                            VectorIndexParameter.MetricType.valueOf(ivfFlatParam.getMetricType().name()),
                                            ivfFlatParam.getNcentroids())));
                case VECTOR_INDEX_TYPE_IVF_PQ:
                    Common.CreateIvfPqParam pqParam = vectorParam.getIvfPqParameter();
                    return new IndexParameter(
                            IndexParameter.IndexType.valueOf(parameter.getIndexType().name()),
                            new VectorIndexParameter(VectorIndexParameter.VectorIndexType.VECTOR_INDEX_TYPE_IVF_PQ,
                                    new IvfPqParam(
                                            pqParam.getDimension(),
                                            VectorIndexParameter.MetricType.valueOf(pqParam.getMetricType().name()),
                                            pqParam.getNcentroids(),
                                            pqParam.getNsubvector(),
                                            pqParam.getBucketInitSize(),
                                            pqParam.getBucketMaxSize(),
                                            pqParam.getNbitsPerIdx()
                                    )
                            )
                    );
                case VECTOR_INDEX_TYPE_HNSW:
                    Common.CreateHnswParam hnswParam = vectorParam.getHnswParameter();
                    return new IndexParameter(
                            IndexParameter.IndexType.valueOf(parameter.getIndexType().name()),
                            new VectorIndexParameter(VectorIndexParameter.VectorIndexType.VECTOR_INDEX_TYPE_HNSW,
                                    new HnswParam(hnswParam.getDimension(),
                                            VectorIndexParameter.MetricType.valueOf(hnswParam.getMetricType().name()),
                                            hnswParam.getEfConstruction(),
                                            hnswParam.getMaxElements(),
                                            hnswParam.getNlinks())));
                case VECTOR_INDEX_TYPE_DISKANN:
                    Common.CreateDiskAnnParam annParam = vectorParam.getDiskannParameter();
                    return new IndexParameter(
                            IndexParameter.IndexType.valueOf(parameter.getIndexType().name()),
                            new VectorIndexParameter(VectorIndexParameter.VectorIndexType.VECTOR_INDEX_TYPE_DISKANN,
                                    new DiskAnnParam(
                                            annParam.getDimension(),
                                            VectorIndexParameter.MetricType.valueOf(annParam.getMetricType().name()))));
                case VECTOR_INDEX_TYPE_BRUTEFORCE:
                    Common.CreateBruteForceParam bruteForceParam = vectorParam.getBruteforceParameter();
                    return new IndexParameter(
                            IndexParameter.IndexType.valueOf(parameter.getIndexType().name()),
                            new VectorIndexParameter(VectorIndexParameter.VectorIndexType.VECTOR_INDEX_TYPE_BRUTEFORCE,
                                    new BruteForceParam(
                                            bruteForceParam.getDimension(),
                                            VectorIndexParameter.MetricType.valueOf(bruteForceParam.getMetricType().name()))));
                default:
                    throw new IllegalStateException("Unexpected value: " + vectorParam.getVectorIndexType());
            }
        }
        if (parameter.getIndexType() == INDEX_TYPE_SCALAR) {
            // Scalar parameter
            Common.ScalarIndexParameter scalarParam = parameter.getScalarIndexParameter();
            return new IndexParameter(IndexParameter.IndexType.valueOf(parameter.getIndexType().name()),
                    new ScalarIndexParameter(
                            ScalarIndexParameter.ScalarIndexType.valueOf(scalarParam.getScalarIndexType().name()),
                            scalarParam.getIsUnique()));

        }
        return null;
    }

    public static Common.IndexParameter mapping(IndexParameter parameter) {
        Common.IndexParameter.Builder builder = Common.IndexParameter.newBuilder();
        if (parameter.getIndexType().equals(IndexParameter.IndexType.INDEX_TYPE_VECTOR)) {
            VectorIndexParameter vectorParameter = parameter.getVectorIndexParameter();
            Common.VectorIndexParameter.Builder build = Common.VectorIndexParameter.newBuilder()
                    .setVectorIndexType(Common.VectorIndexType.valueOf(vectorParameter.getVectorIndexType().name()));
            switch (vectorParameter.getVectorIndexType()) {
                case VECTOR_INDEX_TYPE_FLAT:
                    build.setFlatParameter(Common.CreateFlatParam.newBuilder()
                            .setDimension(vectorParameter.getFlatParam().getDimension())
                            .setMetricType(Common.MetricType.valueOf(vectorParameter.getFlatParam().getMetricType().name()))
                            .build());
                    break;
                case VECTOR_INDEX_TYPE_IVF_FLAT:
                    IvfFlatParam ivfFlatParam = vectorParameter.getIvfFlatParam();
                    build.setIvfFlatParameter(Common.CreateIvfFlatParam.newBuilder()
                            .setDimension(ivfFlatParam.getDimension())
                            .setMetricType(Common.MetricType.valueOf(ivfFlatParam.getMetricType().name()))
                            .setNcentroids(ivfFlatParam.getNcentroids())
                            .build());
                    break;
                case VECTOR_INDEX_TYPE_IVF_PQ:
                    IvfPqParam ivfPqParam = vectorParameter.getIvfPqParam();
                    build.setIvfPqParameter(Common.CreateIvfPqParam.newBuilder()
                            .setDimension(ivfPqParam.getDimension())
                            .setMetricType(Common.MetricType.valueOf(ivfPqParam.getMetricType().name()))
                            .setNcentroids(ivfPqParam.getNcentroids())
                            .setBucketInitSize(ivfPqParam.getBucketInitSize())
                            .setBucketMaxSize(ivfPqParam.getBucketMaxSize())
                            .setNbitsPerIdx(ivfPqParam.getNbitsPerIdx())
                            .setNsubvector(ivfPqParam.getNsubvector())
                            .build());
                    break;
                case VECTOR_INDEX_TYPE_HNSW:
                    HnswParam hnswParam = vectorParameter.getHnswParam();
                    build.setHnswParameter(Common.CreateHnswParam.newBuilder()
                            .setDimension(hnswParam.getDimension())
                            .setMetricType(Common.MetricType.valueOf(hnswParam.getMetricType().name()))
                            .setEfConstruction(hnswParam.getEfConstruction())
                            .setMaxElements(hnswParam.getMaxElements())
                            .setNlinks(hnswParam.getNlinks()).build());
                    break;
                case VECTOR_INDEX_TYPE_DISKANN:
                    DiskAnnParam diskAnnParam = vectorParameter.getDiskAnnParam();
                    build.setDiskannParameter(Common.CreateDiskAnnParam.newBuilder()
                            .setDimension(diskAnnParam.getDimension())
                            .setMetricType(Common.MetricType.valueOf(diskAnnParam.getMetricType().name()))
                            .build());
                    break;
                case VECTOR_INDEX_TYPE_BRUTEFORCE:
                    BruteForceParam bruteForceParam = vectorParameter.getBruteForceParam();
                    build.setBruteforceParameter(Common.CreateBruteForceParam.newBuilder()
                            .setDimension(bruteForceParam.getDimension())
                            .setMetricType(Common.MetricType.valueOf(bruteForceParam.getMetricType().name()))
                            .build());
            }
            builder.setVectorIndexParameter(build.build());
        } else {
            ScalarIndexParameter scalarParameter = parameter.getScalarIndexParameter();
            Common.ScalarIndexParameter scalarIndexParameter = Common.ScalarIndexParameter.newBuilder()
                    .setScalarIndexType(Common.ScalarIndexType.valueOf(scalarParameter.getScalarIndexType().name()))
                    .setIsUnique(scalarParameter.isUnique())
                    .build();
            builder.setScalarIndexParameter(scalarIndexParameter);
        }
        return builder.setIndexType(Common.IndexType.valueOf(parameter.getIndexType().name())).build();
    }

    public static Common.VectorWithId mapping(VectorWithId withId) {
        Common.VectorWithId.Builder builder = Common.VectorWithId.newBuilder()
                .setId(withId.getId());
        if (withId.getVector() != null) {
            Vector vector = withId.getVector();
            builder.setVector(mapping(vector));
        }
        if (withId.getScalarData() != null) {
            builder.setScalarData(mapping(withId.getScalarData()));
        }
        if (withId.getTableData() != null) {
            VectorTableData tableData = withId.getTableData();
            builder.setTableData(Common.VectorTableData.newBuilder()
                    .setTableKey(ByteString.copyFrom(tableData.getKey()))
                    .setTableValue(ByteString.copyFrom(tableData.getValue()))
                    .build());
        }
        return builder.build();
    }

    public static Common.Vector mapping(Vector vector) {
        return Common.Vector.newBuilder()
                .setDimension(vector.getDimension())
                .setValueType(Common.ValueType.valueOf(vector.getValueType().name()))
                .addAllFloatValues(vector.getFloatValues())
                .addAllBinaryValues(vector.getBinaryValues()
                        .stream()
                        .map(ByteString::copyFrom)
                        .collect(Collectors.toList()))
                .build();
    }

    public static Vector mapping(Common.Vector vector) {
        return new Vector(
            vector.getDimension(),
            Vector.ValueType.valueOf(vector.getValueType().name()),
            vector.getFloatValuesList(),
            vector.getBinaryValuesList().stream().map(ByteString::toByteArray).collect(Collectors.toList()));
    }

    public static Common.VectorScalardata mapping(Map<String, ScalarValue> scalarData) {
        return Common.VectorScalardata.newBuilder().putAllScalarData(scalarData.entrySet().stream()
                    .collect(
                            Maps::newHashMap,
                            (map, entry) -> map.put(entry.getKey(), mapping(entry.getValue())),
                            Map::putAll))
                .build();
    }

    public static VectorWithId mapping(Common.VectorWithId withId) {
        Common.Vector vector = withId.getVector();
        return new VectorWithId(withId.getId(), mapping(vector),
                withId.getScalarData().getScalarDataMap().entrySet().stream().collect(
                        Maps::newHashMap,
                        (map, entry) -> map.put(entry.getKey(), mapping(entry.getValue())),
                        Map::putAll),
                new VectorTableData(
                        withId.getTableData().getTableKey().toByteArray(),
                        withId.getTableData().getTableValue().toByteArray())
        );
    }

    public static IndexMetrics mapping(Meta.IndexMetrics metrics) {
        return new IndexMetrics(
                metrics.getRowsCount(),
                metrics.getMinKey().toByteArray(),
                metrics.getMaxKey().toByteArray(),
                metrics.getPartCount(),
                VectorIndexParameter.VectorIndexType.valueOf(metrics.getVectorIndexType().name()),
                metrics.getCurrentCount(),
                metrics.getDeletedCount(),
                metrics.getMaxId(),
                metrics.getMinId(),
                metrics.getMemoryBytes());
    }

    public static VectorIndexMetrics mapping(Common.VectorIndexMetrics metrics) {
        return new VectorIndexMetrics(
                VectorIndexParameter.VectorIndexType.valueOf(metrics.getVectorIndexType().name()),
                metrics.getCurrentCount(),
                metrics.getDeletedCount(),
                metrics.getMaxId(),
                metrics.getMinId(),
                metrics.getMemoryBytes());
    }

    public static ScalarValue mapping(Common.ScalarValue value) {
        return new ScalarValue(ScalarValue.ScalarFieldType.valueOf(value.getFieldType().name()),
                value.getFieldsList().stream()
                        .map(f -> mapping(f, value.getFieldType()))
                        .collect(Collectors.toList()));
    }

    public static ScalarField mapping(Common.ScalarField field, Common.ScalarFieldType type) {
        switch (type) {
            case BOOL:
                return new ScalarField(field.getBoolData());
            case INT8:
            case INT16:
            case INT32:
                return new ScalarField(field.getIntData());
            case INT64:
                return new ScalarField(field.getLongData());
            case FLOAT32:
                return new ScalarField(field.getFloatData());
            case DOUBLE:
                return new ScalarField(field.getDoubleData());
            case STRING:
                return new ScalarField(field.getStringData());
            case BYTES:
                return new ScalarField(field.getBytesData().toByteArray());
            default:
                throw new IllegalStateException("Unexpected value: " + type);
        }
    }

    public static Common.ScalarValue mapping(ScalarValue value) {
        return Common.ScalarValue.newBuilder()
                .setFieldType(Common.ScalarFieldType.valueOf(value.getFieldType().name()))
                .addAllFields(value.getFields().stream().map(f -> mapping(f, value.getFieldType()))
                        .collect(Collectors.toList()))
                .build();
    }

    public static Common.ScalarField mapping(ScalarField field, ScalarValue.ScalarFieldType type) {
        switch (type) {
            case BOOL:
                return Common.ScalarField.newBuilder().setBoolData((Boolean) field.getData()).build();
            case INTEGER:
                return Common.ScalarField.newBuilder().setIntData((Integer) field.getData()).build();
            case LONG:
                return Common.ScalarField.newBuilder().setLongData((Long) field.getData()).build();
            case FLOAT:
                return Common.ScalarField.newBuilder().setFloatData((Float) field.getData()).build();
            case DOUBLE:
                return Common.ScalarField.newBuilder().setDoubleData((Double) field.getData()).build();
            case STRING:
                return Common.ScalarField.newBuilder().setStringData(field.getData().toString()).build();
            case BYTES:
                return Common.ScalarField.newBuilder().setBytesData(ByteString.copyFromUtf8(field.getData().toString())).build();
            default:
                throw new IllegalStateException("Unexpected value: " + type);
        }
    }

    public static VectorWithDistance mapping(Common.VectorWithDistance distance) {
        return new VectorWithDistance(
                mapping(distance.getVectorWithId()),
                distance.getDistance(),
                VectorIndexParameter.MetricType.valueOf(distance.getMetricType().name()));
    }

    public static Common.CoprocessorV2 mapping(Coprocessor coprocessor, long partId) {
        Common.CoprocessorV2.SchemaWrapper schemaWrapper = Common.CoprocessorV2.SchemaWrapper.newBuilder()
                .addAllSchema(mapping(coprocessor.getOriginalSchema()))
                .setCommonId(partId)
                .build();
        return Common.CoprocessorV2.newBuilder()
                .setSchemaVersion(coprocessor.getSchemaVersion())
                .setOriginalSchema(schemaWrapper)
                .addAllSelectionColumns(Parameters.cleanNull(coprocessor.getSelection(), Collections.emptyList()))
                .setRelExpr(ByteString.copyFrom(Parameters.cleanNull(coprocessor.getExpression(), ByteArrayUtils.EMPTY_BYTES)))
                .build();
    }

    public static List<Common.Schema> mapping(Coprocessor.SchemaWrapper schemaWrapper) {
        if (schemaWrapper == null) {
            return Collections.emptyList();
        }
        return CodecUtils.createSchemaForColumns(schemaWrapper.getSchemas()).stream()
                .map(schema -> {
                    Common.Schema.Type vs;
                    switch (schema.getType()) {
                        case BOOLEAN:
                            vs = Common.Schema.Type.BOOL;
                            break;
                        case INTEGER:
                            vs = Common.Schema.Type.INTEGER;
                            break;
                        case FLOAT:
                            vs = Common.Schema.Type.FLOAT;
                            break;
                        case LONG:
                            vs = Common.Schema.Type.LONG;
                            break;
                        case DOUBLE:
                            vs = Common.Schema.Type.DOUBLE;
                            break;
                        case BYTES:
                        case STRING:
                            vs = Common.Schema.Type.STRING;
                            break;
                        case BOOLEANLIST:
                            vs = Common.Schema.Type.BOOLLIST;
                            break;
                        case INTEGERLIST:
                            vs = Common.Schema.Type.INTEGERLIST;
                            break;
                        case FLOATLIST:
                            vs = Common.Schema.Type.FLOATLIST;
                            break;
                        case LONGLIST:
                            vs = Common.Schema.Type.LONGLIST;
                            break;
                        case DOUBLELIST:
                            vs = Common.Schema.Type.DOUBLELIST;
                            break;
                        case STRINGLIST:
                            vs = Common.Schema.Type.STRINGLIST;
                            break;
                        default:
                            throw new IllegalStateException("Unexpected value: " + schema.getType());
                    }
                    return Common.Schema.newBuilder()
                            .setType(vs)
                            .setIsKey(schema.isKey())
                            .setIsNullable(schema.isAllowNull())
                            .setIndex(schema.getIndex())
                            .build();
                }).collect(Collectors.toList());
    }

    public static Store.Coprocessor mapping(Coprocessor coprocessor, DingoCommonId regionId) {
        return Store.Coprocessor.newBuilder()
                .setSchemaVersion(coprocessor.getSchemaVersion())
                .setOriginalSchema(mapping(coprocessor.getOriginalSchema(), regionId.parentId()))
                .setResultSchema(mapping(coprocessor.getResultSchema(), coprocessor.getResultSchema().getCommonId()))
                .addAllSelectionColumns(coprocessor.getSelection())
                .setExpression(ByteString.copyFrom(coprocessor.getExpression()))
                .addAllGroupByColumns(coprocessor.getGroupBy())
                .addAllAggregationOperators(coprocessor.getAggregations().stream()
                        .map(EntityConversion::mapping)
                        .collect(Collectors.toList()))
                .build();
    }

    public static Store.Coprocessor.SchemaWrapper mapping(Coprocessor.SchemaWrapper schemaWrapper, long schemaId) {
        return Store.Coprocessor.SchemaWrapper.newBuilder()
                .addAllSchema(CodecUtils.createSchemaForColumns(schemaWrapper.getSchemas()).stream()
                        .map(EntityConversion::mapping)
                        .collect(Collectors.toList()))
                .setCommonId(schemaId)
                .build();
    }

    public static Store.AggregationOperator mapping(AggregationOperator aggregationOperator) {
        return Store.AggregationOperator.newBuilder()
                .setOper(Store.AggregationType.forNumber(aggregationOperator.getOperation().getCode()))
                .setIndexOfColumn(aggregationOperator.getIndexOfColumn())
                .build();
    }

    public static Common.Schema mapping(DingoSchema schema) {
        return Common.Schema.newBuilder()
                .setType(mapping(schema.getType()))
                .setIsKey(schema.isKey())
                .setIsNullable(schema.isAllowNull())
                .setIndex(schema.getIndex())
                .build();
    }

    public static Common.Schema.Type mapping(Type type) {
        switch (type) {
            case BOOLEAN:
                return Common.Schema.Type.BOOL;
            case INTEGER:
                return Common.Schema.Type.INTEGER;
            case FLOAT:
                return Common.Schema.Type.FLOAT;
            case LONG:
                return Common.Schema.Type.LONG;
            case DOUBLE:
                return Common.Schema.Type.DOUBLE;
            case BYTES:
            case STRING:
                return Common.Schema.Type.STRING;
            case BOOLEANLIST:
                return Common.Schema.Type.BOOLLIST;
            case INTEGERLIST:
                return Common.Schema.Type.INTEGERLIST;
            case FLOATLIST:
                return Common.Schema.Type.FLOATLIST;
            case LONGLIST:
                return Common.Schema.Type.LONGLIST;
            case DOUBLELIST:
                return Common.Schema.Type.DOUBLELIST;
            case STRINGLIST:
                return Common.Schema.Type.STRINGLIST;
            default:
                throw new IllegalStateException("Unexpected value: " + type);
        }
    }

    public static Meta.PartitionRule calcRange(
            Table table,
            Meta.DingoCommonId tableId,
            List<Meta.DingoCommonId> partitionIds) {
        DingoCommonId tableId1 = mapping(tableId);
        List<Column> keyColumns = table.getKeyColumns();
        keyColumns.sort(Comparator.comparingInt(Column::getPrimary));
        KeyValueCodec codec = DingoKeyValueCodec.of(tableId1.entityId(), keyColumns);
        byte[] minKeyPrefix = codec.encodeMinKeyPrefix();
        byte[] maxKeyPrefix = codec.encodeMaxKeyPrefix();
        Meta.PartitionRule.Builder builder = Meta.PartitionRule.newBuilder();
        boolean isTxn = Parameters.cleanNull(table.getEngine(), "").startsWith("TXN");
        minKeyPrefix[0] = (byte) (isTxn ? 't' : 'r');
        maxKeyPrefix[0] = (byte) (isTxn ? 't' : 'r');
        Iterator<byte[]> keys = Optional.<Partition, Stream<byte[]>>mapOrGet(
                table.getPartition(), __ -> encodePartitionDetails(__.getDetails(), codec), Stream::empty
            ).sorted(ByteArrayUtils::compare)
            .peek($ -> $[0] = (byte) (isTxn ? 't' : 'r'))
            .iterator();

        byte[] start = minKeyPrefix;
        for (Meta.DingoCommonId id : partitionIds) {
            builder.addPartitions(Meta.Partition.newBuilder()
                    .setId(id)
                    .setRange(Common.Range.newBuilder()
                            .setStartKey(ByteString.copyFrom(codec.resetPrefix(start, id.getEntityId())))
                            .setEndKey(ByteString.copyFrom(codec.resetPrefix(maxKeyPrefix, id.getEntityId() + 1)))
                            .build())
                    .build());
            start = keys.hasNext() ? keys.next() : start;
        }
        getStrategy(table.getPartition(), builder);
        return builder.build();
    }

    private static Stream<byte[]> encodePartitionDetails(List<PartitionDetail> details, KeyValueCodec codec) {
        return Parameters.<List<PartitionDetail>>cleanNull(details, Collections::emptyList).stream()
                .map(PartitionDetail::getOperand)
                .map(NoBreakFunctions
                        .<Object[], byte[]>wrap(operand -> codec.encodeKeyPrefix(operand, operand.length), NoBreakFunctions.throwException()));
    }

    public static Meta.ColumnDefinition mapping(Column column) {
        return Meta.ColumnDefinition.newBuilder()
                .setName(column.getName())
                .setNullable(column.isNullable())
                .setElementType(cleanNull(column.getElementType(), ""))
                .setDefaultVal(cleanNull(column.getDefaultValue(), ""))
                .setPrecision(column.getPrecision())
                .setScale(column.getScale())
                .setIndexOfKey(column.getPrimary())
                .setSqlType(column.getType())
                .setIsAutoIncrement(column.isAutoIncrement())
                .setState(column.getState() == 0 ? 1 : column.getState())
                .setComment(Parameters.cleanNull(column.getComment(), ""))
                .build();
    }
    
    public static InternalCoordinator mapping(Location location, Location leaderLocation) {
        boolean isLeader = false;
        if (location.equals(leaderLocation)) {
            isLeader = true;
        }
        return new InternalCoordinator(location, isLeader);
    }

    public static InternalStore mapping(Common.Store store) {
        return new InternalStore(store.getId(),
                store.getStoreType().getNumber(),
                store.getState().getNumber(),
                mapping(store.getServerLocation()),
                mapping(store.getRaftLocation())
        );
    }

    public static InternalRegion mapping(Common.Region region) {
        long leaderStoreId = region.getLeaderStoreId();
        List<Common.Peer> peerList = region.getDefinition().getPeersList();
        List<Location> followers = null;
        Location leader = null;
        if (peerList != null && peerList.size() > 0) {
            followers = peerList.stream().filter(p -> p.getStoreId() != leaderStoreId)
                    .map(peer -> mapping(peer.getRaftLocation()))
                    .collect(Collectors.toList());
            leader = peerList.stream()
                    .filter(p -> p.getStoreId() == leaderStoreId)
                    .map(peer -> mapping(peer.getRaftLocation()))
                    .findAny()
                    .orElseThrow(() -> new DingoClientException("Not found region leader"));
        }
        int regionType = region.getRegionTypeValue();
        int regionState = region.getStateValue();
        long createTime = region.getCreateTimestamp();
        long deleteTime = region.getDeletedTimestamp();
        return new InternalRegion(
                region.getId(),
                regionState,
                regionType,
                createTime,
                deleteTime,
                followers,
                leader,
                leaderStoreId);
    }

}
