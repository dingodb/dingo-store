/*
 * Copyright 2021, Zetyun DataPortal All rights reserved.
 */

package io.dingodb.sdk.common.utils;

import com.google.protobuf.ByteString;
import io.dingodb.common.Common;
import io.dingodb.meta.Meta;
import io.dingodb.sdk.common.codec.KeyValueCodec;
import io.dingodb.sdk.common.partition.PartitionDetailDefinition;
import io.dingodb.sdk.common.table.Column;
import io.dingodb.sdk.common.table.ColumnDefinition;
import io.dingodb.sdk.common.table.Table;
import io.dingodb.sdk.common.table.TableDefinition;

import java.util.Iterator;
import java.util.List;
import java.util.TreeSet;
import java.util.stream.Collectors;

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
import static io.dingodb.sdk.common.utils.ByteArrayUtils.EMPTY_BYTES;

public class EntityConversion {

    public static Meta.TableDefinition swap(Table table) {
        List<Meta.ColumnDefinition> columnDefinitions = table.getColumns().stream()
                .map(EntityConversion::swap)
                .collect(Collectors.toList());

        return Meta.TableDefinition.newBuilder()
                .setName(table.getName())
                .setVersion(table.getVersion())
                .setTtl(table.getTtl())
                .setTablePartition(calcRange(table))
                .setEngine(Common.Engine.valueOf(table.getEngine()))
                .addAllColumns(columnDefinitions).build();
    }

    public static Table swap(Meta.TableDefinition tableDefinition) {
        return new TableDefinition(
                tableDefinition.getName(),
                tableDefinition.getColumnsList().stream().map(EntityConversion::swap).collect(Collectors.toList()),
                tableDefinition.getVersion(),
                (int) tableDefinition.getTtl(),
                null,
                tableDefinition.getEngine().name(),
                tableDefinition.getPropertiesMap());
    }

    public static Column swap(Meta.ColumnDefinition definition) {
        return new ColumnDefinition(
                definition.getName(),
                swap(definition.getSqlType()),
                definition.getElementType().name(),
                definition.getPrecision(),
                definition.getScale(),
                definition.getNullable(),
                definition.getIndexOfKey(),
                definition.getDefaultVal()
        );
    }

    public static Meta.PartitionRule calcRange(Table table) {
        List<Integer> keyList = table.getKeyColumnIndices();
        int columnCount = table.getColumns().size();
        List<PartitionDetailDefinition> partDetails = table.getPartDefinition().details();

        List<Column> cols = keyList.stream().map(table::getColumn).collect(Collectors.toList());

        for (PartitionDetailDefinition partDetail : partDetails) {
            if (partDetail.getOperand().size() > keyList.size()) {
                throw new IllegalArgumentException(
                    "Partition values count must be <= key columns count, but values count is "
                    + partDetail.getOperand().size()
                );
            }
            for (int i = 0; i < partDetail.getOperand().size(); i++) {
                String simpleName = partDetail.getOperand().get(i).getClass().getSimpleName();
                String sqlType = cols.get(i).getType();
                if (!simpleName.equalsIgnoreCase(sqlType)) {
                    throw new IllegalArgumentException(
                        "partition value type: (" + simpleName + ") must be the same as the primary key type: (" + sqlType + ")"
                    );
                }
            }
        }

        KeyValueCodec codec = table.createCodec();
        Iterator<byte[]> keys = partDetails.stream()
                .map(PartitionDetailDefinition::getOperand)
                .map(operand -> operand.toArray(new Object[columnCount]))
                .map(NoBreakFunctions.wrap(codec::encodeKey))
                .collect(Collectors.toCollection(() -> new TreeSet<>(ByteArrayUtils::compare)))
                .iterator();

        byte[] start = EMPTY_BYTES;
        Meta.RangePartition.Builder rangePartitionBuilder = Meta.RangePartition.newBuilder();
        while (keys.hasNext()) {
            rangePartitionBuilder
                    .addRanges(Common.Range.newBuilder()
                            .setStartKey(ByteString.copyFrom(start))
                            .setEndKey(ByteString.copyFrom(start = keys.next()))
                            .build());
        }
        Common.Range range = Common.Range.newBuilder()
                .setStartKey(ByteString.copyFrom(start))
                .build();
        Meta.RangePartition rangePartition = rangePartitionBuilder.addRanges(range).build();

        return Meta.PartitionRule.newBuilder()
                .setRangePartition(rangePartition)
                .build();
    }

    public static Meta.ColumnDefinition swap(Column column) {
        return Meta.ColumnDefinition.newBuilder()
                .setName(column.getName())
                .setNullable(column.isNullable())
                .setElementType(Meta.ElementType.ELEM_TYPE_STRING)
                .setDefaultVal(column.getDefaultValue())
                .setPrecision(column.getPrecision())
                .setScale(column.getScale())
                .setIndexOfKey(column.getPrimary())
                .setSqlType(swap(column.getType()))
                .build();
    }

    private static String swap(Meta.SqlType sqlType) {
        switch (sqlType) {
            case SQL_TYPE_VARCHAR:
                return "STRING";
            case SQL_TYPE_INTEGER:
                return "INTEGER";
            case SQL_TYPE_BOOLEAN:
                return "BOOLEAN";
            case SQL_TYPE_DOUBLE:
                return "DOUBLE";
            case SQL_TYPE_BIGINT:
                return "LONG";
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

    private static Meta.SqlType swap(String type) {
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
