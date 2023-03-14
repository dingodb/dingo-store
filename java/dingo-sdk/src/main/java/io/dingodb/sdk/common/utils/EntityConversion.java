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

import static io.dingodb.meta.Meta.SqlType.SQL_TYPE_INTEGER;
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
        int columnCount = table.getColumns().size();
        List<PartitionDetailDefinition> partDetails = table.getPartDefinition().details();

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
            default:
                return "STRING";
        }
    }

    private static Meta.SqlType swap(String type) {
        switch (type.toLowerCase()) {
            case "varchar":
                return Meta.SqlType.SQL_TYPE_VARCHAR;
            case "integer":
                return SQL_TYPE_INTEGER;
            case "boolean":
                return Meta.SqlType.SQL_TYPE_BOOLEAN;
            case "long":
                return Meta.SqlType.SQL_TYPE_BIGINT;
            case "double":
                return Meta.SqlType.SQL_TYPE_DOUBLE;
            default:
                return Meta.SqlType.SQL_TYPE_VARCHAR;
        }
    }
}
