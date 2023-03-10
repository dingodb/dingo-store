/*
 * Copyright 2021, Zetyun DataPortal All rights reserved.
 */

package io.dingodb.sdk.common.utils;

import com.google.protobuf.ByteString;
import io.dingodb.common.Common;
import io.dingodb.meta.Meta;
import io.dingodb.sdk.common.partition.PartitionDetail;
import io.dingodb.sdk.common.table.Column;
import io.dingodb.sdk.common.table.Table;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.TreeSet;
import java.util.stream.Collectors;

public class EntityConversion {

    public static final byte[] EMPTY_BYTES = new byte[0];

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

    public static Meta.PartitionRule calcRange(Table table) {
        int primaryKeyCount = table.getPrimaryKeyCount();
        List<PartitionDetail> partDetails = table.getPartDefinition().details();

        Iterator<byte[]> keys = partDetails.stream()
                .map(PartitionDetail::getOperand)
                .map(operand -> operand.toArray(new Object[primaryKeyCount]))
                .map(obs -> Arrays.toString(obs).getBytes(StandardCharsets.UTF_8))
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

    private static Meta.SqlType swap(String type) {
        switch (type.toLowerCase()) {
            case "varchar":
                return Meta.SqlType.SQL_TYPE_VARCHAR;
            case "integer":
                return Meta.SqlType.SQL_TYPE_INTEGER;
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
