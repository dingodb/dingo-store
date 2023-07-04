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
import io.dingodb.sdk.common.cluster.InternalExecutor;
import io.dingodb.sdk.common.cluster.InternalExecutorMap;
import io.dingodb.sdk.common.cluster.InternalExecutorUser;
import io.dingodb.sdk.common.codec.CodecUtils;
import io.dingodb.sdk.common.codec.DingoKeyValueCodec;
import io.dingodb.sdk.common.codec.KeyValueCodec;
import io.dingodb.sdk.common.index.DiskAnnParam;
import io.dingodb.sdk.common.index.FlatParam;
import io.dingodb.sdk.common.index.HnswParam;
import io.dingodb.sdk.common.index.Index;
import io.dingodb.sdk.common.index.IndexDefinition;
import io.dingodb.sdk.common.index.IndexParameter;
import io.dingodb.sdk.common.index.IvfFlatParam;
import io.dingodb.sdk.common.index.IvfPqParam;
import io.dingodb.sdk.common.index.VectorIndexParameter;
import io.dingodb.sdk.common.partition.Partition;
import io.dingodb.sdk.common.partition.PartitionDetail;
import io.dingodb.sdk.common.partition.PartitionDetailDefinition;
import io.dingodb.sdk.common.partition.PartitionRule;
import io.dingodb.sdk.common.serial.RecordEncoder;
import io.dingodb.sdk.common.serial.schema.DingoSchema;
import io.dingodb.sdk.common.serial.schema.Type;
import io.dingodb.sdk.common.table.Column;
import io.dingodb.sdk.common.table.ColumnDefinition;
import io.dingodb.sdk.common.table.RangeDistribution;
import io.dingodb.sdk.common.table.Table;
import io.dingodb.sdk.common.table.TableDefinition;
import io.dingodb.sdk.common.table.metric.TableMetrics;
import io.dingodb.sdk.common.vector.Vector;
import io.dingodb.sdk.common.vector.VectorWithDistance;
import io.dingodb.sdk.common.vector.VectorWithId;
import io.dingodb.sdk.service.store.AggregationOperator;
import io.dingodb.sdk.service.store.Coprocessor;
import io.dingodb.store.Store;

import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.dingodb.meta.Meta.SqlType.SQL_TYPE_ANY;
import static io.dingodb.meta.Meta.SqlType.SQL_TYPE_ARRAY;
import static io.dingodb.meta.Meta.SqlType.SQL_TYPE_BIGINT;
import static io.dingodb.meta.Meta.SqlType.SQL_TYPE_BOOLEAN;
import static io.dingodb.meta.Meta.SqlType.SQL_TYPE_BYTES;
import static io.dingodb.meta.Meta.SqlType.SQL_TYPE_DATE;
import static io.dingodb.meta.Meta.SqlType.SQL_TYPE_DOUBLE;
import static io.dingodb.meta.Meta.SqlType.SQL_TYPE_FLOAT;
import static io.dingodb.meta.Meta.SqlType.SQL_TYPE_INTEGER;
import static io.dingodb.meta.Meta.SqlType.SQL_TYPE_MULTISET;
import static io.dingodb.meta.Meta.SqlType.SQL_TYPE_TIME;
import static io.dingodb.meta.Meta.SqlType.SQL_TYPE_TIMESTAMP;
import static io.dingodb.meta.Meta.SqlType.SQL_TYPE_VARCHAR;
import static io.dingodb.sdk.common.utils.NoBreakFunctions.wrap;
import static io.dingodb.sdk.common.utils.Parameters.cleanNull;

public class EntityConversion {

    public static Meta.TableDefinition mapping(Table table, Meta.DingoCommonId tableId) {
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
                .setTablePartition(calcRange(table, tableId))
                .setEngine(Common.Engine.valueOf(table.getEngine()))
                .setReplica(table.getReplica())
                .addAllColumns(columnDefinitions)
                .setAutoIncrement(table.getAutoIncrement())
                .setCreateSql(Parameters.cleanNull(table.getCreateSql(), ""))
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
                .partition(null)
                .engine(tableDefinition.getEngine().name())
                .properties(tableDefinition.getPropertiesMap())
                .partition(mapping(tableDefinitionWithId.getTableId().getEntityId(), tableDefinition, columns))
                .replica(tableDefinition.getReplica())
                .autoIncrement(tableDefinition.getAutoIncrement())
                .createSql(tableDefinition.getCreateSql())
                .build();
    }

    public static Partition mapping(long id, Meta.TableDefinition tableDefinition, List<Column> columns) {
        Meta.PartitionRule partition = tableDefinition.getTablePartition();
        if (partition.getRangePartition().getRangesCount() <= 1) {
            return null;
        }
        DingoKeyValueCodec codec = DingoKeyValueCodec.of(id, columns);
        List<PartitionDetail> details = partition.getRangePartition().getRangesList().stream()
                .map(Common.Range::getStartKey)
                .map(ByteString::toByteArray)
                .sorted(ByteArrayUtils::compare)
                .skip(1)
                .map(wrap(codec::decodeKeyPrefix))
                .map(key -> new PartitionDetailDefinition(null, null, key))
                .collect(Collectors.toList());
        // The current version only supports the range strategy.
        return new PartitionRule("RANGE", partition.getColumnsList(), details);
    }

    public static Column mapping(Meta.ColumnDefinition definition) {
        return ColumnDefinition.builder()
                .name(definition.getName())
                .type(mapping(definition.getSqlType()))
                .elementType(definition.getElementType().name())
                .precision(definition.getPrecision())
                .scale(definition.getScale())
                .nullable(definition.getNullable())
                .primary(definition.getIndexOfKey())
                .defaultValue(definition.getDefaultVal())
                .isAutoIncrement(definition.getIsAutoIncrement())
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

    public static RangeDistribution mapping(Meta.RangeDistribution rangeDistribution) {
        return new RangeDistribution(
                mapping(rangeDistribution.getId()),
                mapping(rangeDistribution.getRange()),
                mapping(rangeDistribution.getLeader()),
                rangeDistribution.getVotersList().stream().map(EntityConversion::mapping).collect(Collectors.toList()));
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

    public static Index mapping(Meta.IndexDefinition definition) {
        return new IndexDefinition(
                definition.getName(),
                definition.getVersion(),
                null,
                definition.getReplica(),
                mapping(definition.getIndexParameter()),
                definition.getWithAutoIncrment(),
                definition.getAutoIncrement());
    }

    public static Meta.IndexDefinition mapping(long id, Index index) {
        RecordEncoder re = new RecordEncoder(0, id);
        return Meta.IndexDefinition.newBuilder()
                .setName(index.getName())
                .setVersion(index.getVersion())
                .setIndexPartition(
                        Meta.PartitionRule.newBuilder()
                                .setRangePartition(Meta.RangePartition.newBuilder()
                                        .addRanges(Common.Range.newBuilder()
                                                .setStartKey(ByteString.copyFrom(re.encodeMinKeyPrefix()))
                                                .build())
                                        .build())
                                .build())
                .setReplica(index.getReplica())
                .setIndexParameter(mapping(index.getIndexParameter()))
                .setWithAutoIncrment(index.isAutoIncrement())
                .setAutoIncrement(index.getAutoIncrement())
                .build();
    }

    public static IndexParameter mapping(Common.IndexParameter parameter) {
        Common.VectorIndexParameter vectorParam = parameter.getVectorIndexParameter();
        switch (vectorParam.getVectorIndexType()) {
            case VECTOR_INDEX_TYPE_FLAT:
                return new IndexParameter(
                        IndexParameter.IndexType.valueOf(parameter.getIndexType().name()),
                        new VectorIndexParameter(VectorIndexParameter.VectorIndexType.VECTOR_INDEX_TYPE_FLAT,
                                new FlatParam(vectorParam.getFlatParameter().getDimension())));
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
                                        pqParam.getBucketMaxSize())));
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

            default:
                throw new IllegalStateException("Unexpected value: " + vectorParam.getVectorIndexType());
        }
    }

    public static Common.IndexParameter mapping(IndexParameter parameter) {
        VectorIndexParameter vectorParameter = parameter.getVectorIndexParameter();
        Common.VectorIndexParameter.Builder build = Common.VectorIndexParameter.newBuilder()
                .setVectorIndexType(Common.VectorIndexType.valueOf(vectorParameter.getVectorIndexType().name()));
        switch (vectorParameter.getVectorIndexType()) {
            case VECTOR_INDEX_TYPE_FLAT:
                build.setFlatParameter(Common.CreateFlatParam.newBuilder()
                        .setDimension(vectorParameter.getFlatParam().getDimension())
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
        }
        return Common.IndexParameter.newBuilder()
                .setIndexType(Common.IndexType.valueOf(parameter.getIndexType().name()))
                .setVectorIndexParameter(build.build())
                .build();

        /*if (parameter.getScalarIndexParameter() != null) {
            ScalarIndexParameter scalarParameter = (ScalarIndexParameter) parameter.getScalarIndexParameter();
            builder.setScalarIndexParameter(
                    Common.ScalarIndexParameter.newBuilder()
                            .setScalarIndexType(Common.ScalarIndexType.valueOf(scalarParameter.getIndexType().name()))
                            .build());
        }*/
    }

    public static Common.VectorWithId mapping(VectorWithId withId) {
        Vector vector = withId.getVector();
        return Common.VectorWithId.newBuilder()
                .setId(withId.getId())
                .setVector(Common.Vector.newBuilder()
                        .setDimension(vector.getDimension())
                        .setValueType(Common.ValueType.valueOf(vector.getValueType().name()))
                        .addAllFloatValues(vector.getFloatValues())
                        .addAllBinaryValues(vector.getBinaryValues()
                                .stream()
                                .map(ByteString::copyFrom)
                                .collect(Collectors.toList()))
                        .build())
                .setMetadata(Common.VectorMetadata.newBuilder().putAllMetadata(withId.getMetaData().entrySet().stream()
                        .collect(
                                Maps::newHashMap,
                                (map, entry) -> map.put(entry.getKey(), ByteString.copyFrom(entry.getValue())),
                                Map::putAll))
                        .build())
                .build();
    }

    public static VectorWithId mapping(Common.VectorWithId withId) {
        Common.Vector vector = withId.getVector();
        return new VectorWithId(withId.getId(), new Vector(
                vector.getDimension(),
                Vector.ValueType.valueOf(vector.getValueType().name()),
                vector.getFloatValuesList(),
                vector.getBinaryValuesList().stream().map(ByteString::toByteArray).collect(Collectors.toList())),
                withId.getMetadata()
                        .getMetadataMap()
                        .entrySet().stream()
                        .collect(
                                Maps::newHashMap,
                                (map, entry) -> map.put(entry.getKey(), entry.getValue().toByteArray()),
                                Map::putAll));
    }

    public static VectorWithDistance mapping(Common.VectorWithDistance distance) {
        return new VectorWithDistance(mapping(distance.getVectorWithId()), distance.getDistance());
    }

    public static Store.Coprocessor mapping(Coprocessor coprocessor) {
        return Store.Coprocessor.newBuilder()
                .setSchemaVersion(coprocessor.getSchemaVersion())
                .setOriginalSchema(mapping(coprocessor.getOriginalSchema()))
                .setResultSchema(mapping(coprocessor.getResultSchema()))
                .addAllSelectionColumns(coprocessor.getSelection())
                .setExpression(ByteString.copyFrom(coprocessor.getExpression()))
                .addAllGroupByColumns(coprocessor.getGroupBy())
                .addAllAggregationOperators(coprocessor.getAggregations().stream()
                        .map(EntityConversion::mapping)
                        .collect(Collectors.toList()))
                .build();
    }

    public static Store.Coprocessor.SchemaWrapper mapping(Coprocessor.SchemaWrapper schemaWrapper) {
        return Store.Coprocessor.SchemaWrapper.newBuilder()
                .addAllSchema(CodecUtils.createSchemaForColumns(schemaWrapper.getSchemas()).stream()
                        .map(EntityConversion::mapping)
                        .collect(Collectors.toList()))
                .setCommonId(schemaWrapper.getCommonId())
                .build();
    }

    public static Store.AggregationOperator mapping(AggregationOperator aggregationOperator) {
        return Store.AggregationOperator.newBuilder()
                .setOper(Store.AggregationType.forNumber(aggregationOperator.getOperation().getCode()))
                .setIndexOfColumn(aggregationOperator.getIndexOfColumn())
                .build();
    }

    public static Store.Schema mapping(DingoSchema schema) {
        return Store.Schema.newBuilder()
                .setType(mapping(schema.getType()))
                .setIsKey(schema.isKey())
                .setIsNullable(schema.isAllowNull())
                .setIndex(schema.getIndex())
                .build();
    }

    public static Store.Schema.Type mapping(Type type) {
        switch (type) {
            case BOOLEAN:
                return Store.Schema.Type.BOOL;
            case INTEGER:
                return Store.Schema.Type.INTEGER;
            case FLOAT:
                return Store.Schema.Type.FLOAT;
            case LONG:
                return Store.Schema.Type.LONG;
            case DOUBLE:
                return Store.Schema.Type.DOUBLE;
            case BYTES:
            case STRING:
                return Store.Schema.Type.STRING;
            default:
                throw new IllegalStateException("Unexpected value: " + type);
        }
    }

    public static Meta.PartitionRule calcRange(Table table, Meta.DingoCommonId tableId) {
        DingoCommonId tableId1 = mapping(tableId);
        List<Column> keyColumns = table.getKeyColumns();
        keyColumns.sort(Comparator.comparingInt(Column::getPrimary));
        KeyValueCodec codec = DingoKeyValueCodec.of(tableId1.entityId(), keyColumns);
        byte[] minKeyPrefix = codec.encodeMinKeyPrefix();
        byte[] maxKeyPrefix = codec.encodeMaxKeyPrefix();
        Meta.RangePartition.Builder rangeBuilder = Meta.RangePartition.newBuilder();

        Iterator<byte[]> keys = Stream.concat(
                Optional.mapOrGet(table.getPartition(), __ -> encodePartitionDetails(__.details(), codec),
                        Stream::empty),
                Stream.of(maxKeyPrefix))
                .sorted(ByteArrayUtils::compare).iterator();

        byte[] start = minKeyPrefix;
        while (keys.hasNext()) {
            rangeBuilder.addRanges(Common.Range.newBuilder()
                    .setStartKey(ByteString.copyFrom(start))
                    .setEndKey(ByteString.copyFrom(start = keys.next()))
                    .build());
        }

        return Meta.PartitionRule.newBuilder().setRangePartition(rangeBuilder.build()).build();
    }

    private static Stream<byte[]> encodePartitionDetails(List<PartitionDetail> details, KeyValueCodec codec) {
        return Parameters.<List<PartitionDetail>>cleanNull(details, Collections::emptyList).stream()
                .map(PartitionDetail::getOperand)
                .map(NoBreakFunctions
                        .<Object[], byte[]>wrap(operand -> codec.encodeKeyPrefix(operand, operand.length)));
    }

    public static Meta.ColumnDefinition mapping(Column column) {
        return Meta.ColumnDefinition.newBuilder()
                .setName(column.getName())
                .setNullable(column.isNullable())
                .setElementType(Meta.ElementType.ELEM_TYPE_STRING)
                .setDefaultVal(cleanNull(column.getDefaultValue(), ""))
                .setPrecision(column.getPrecision())
                .setScale(column.getScale())
                .setIndexOfKey(column.getPrimary())
                .setSqlType(mapping(column.getType()))
                .setIsAutoIncrement(column.isAutoIncrement())
                .build();
    }

    private static String mapping(Meta.SqlType sqlType) {
        switch (sqlType) {
            case SQL_TYPE_VARCHAR:
                return "VARCHAR";
            case SQL_TYPE_INTEGER:
                return "INTEGER";
            case SQL_TYPE_BOOLEAN:
                return "BOOLEAN";
            case SQL_TYPE_DOUBLE:
                return "DOUBLE";
            case SQL_TYPE_BIGINT:
                return "BIGINT";
            case SQL_TYPE_FLOAT:
                return "FLOAT";
            case SQL_TYPE_DATE:
                return "DATE";
            case SQL_TYPE_TIME:
                return "TIME";
            case SQL_TYPE_TIMESTAMP:
                return "TIMESTAMP";
            case SQL_TYPE_ARRAY:
                return "ARRAY";
            case SQL_TYPE_MULTISET:
                return "MULTISET";
            case SQL_TYPE_ANY:
                return "ANY";
            case SQL_TYPE_BYTES:
                return "BINARY";
            default:
                break;
        }
        throw new IllegalArgumentException("Unrecognized type name \"" + sqlType.name() + "\".");
    }

    private static Meta.SqlType mapping(String type) {
        switch (type.toUpperCase()) {
            case "STRING":
            case "VARCHAR":
                return SQL_TYPE_VARCHAR;
            case "INT":
            case "INTEGER":
                return SQL_TYPE_INTEGER;
            case "BOOL":
            case "BOOLEAN":
                return SQL_TYPE_BOOLEAN;
            case "LONG":
            case "BIGINT":
                return SQL_TYPE_BIGINT;
            case "DOUBLE":
                return SQL_TYPE_DOUBLE;
            case "FLOAT":
                return SQL_TYPE_FLOAT;
            case "DATE":
                return SQL_TYPE_DATE;
            case "TIME":
                return SQL_TYPE_TIME;
            case "TIMESTAMP":
                return SQL_TYPE_TIMESTAMP;
            case "ARRAY":
                return SQL_TYPE_ARRAY;
            case "LIST":
            case "MULTISET":
                return SQL_TYPE_MULTISET;
            case "ANY":
                return SQL_TYPE_ANY;
            case "BYTES":
            case "BINARY":
            case "BLOB":
                return SQL_TYPE_BYTES;
            default:
                break;
        }
        throw new IllegalArgumentException("Unrecognized type name \"" + type + "\".");
    }
}
