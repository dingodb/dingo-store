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

package io.dingodb.sdk.common.codec;

import io.dingodb.sdk.common.serial.BufImpl;
import io.dingodb.sdk.common.serial.schema.DingoSchema;
import io.dingodb.sdk.common.table.Column;
import io.dingodb.sdk.common.utils.Parameters;
import io.dingodb.sdk.common.utils.TypeSchemaMapper;
import io.dingodb.sdk.service.entity.meta.ColumnDefinition;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

public final class CodecUtils {

    private CodecUtils() {
    }

    public static int compareColumnByPrimary(int c1, int c2) {
        if (c1 * c2 > 0) {
            return Integer.compare(c1, c2);
        }
        return c1 < 0 ? 1 : c2 < 0 ? -1 : c1 - c2;
    }

    public static Comparator<Column> sortColumnByPrimaryComparator() {
        return (c1, c2) -> compareColumnByPrimary(c1.getPrimary(), c2.getPrimary());
    }

    public static List<Column> sortColumns(List<Column> columns) {
        List<Column> codecOrderColumns = new ArrayList<>(columns);
        codecOrderColumns.sort(sortColumnByPrimaryComparator());
        return codecOrderColumns;
    }

    public static Comparator<ColumnDefinition> sortColumnDefinitionByPrimaryComparator() {
        return (c1, c2) -> compareColumnByPrimary(c1.getIndexOfKey(), c2.getIndexOfKey());
    }

    public static List<ColumnDefinition> sortColumnDefinitions(List<ColumnDefinition> columns) {
        List<ColumnDefinition> codecOrderColumns = new ArrayList<>(columns);
        codecOrderColumns.sort(sortColumnDefinitionByPrimaryComparator());
        return codecOrderColumns;
    }

    public static DingoSchema createSchemaForColumn(Column column) {
        return createSchemaForColumn(column, 0);
    }

    public static DingoSchema createSchemaForColumn(Column column, int index) {
        DingoSchema schema;
        String typeName = column.getType();
        if (typeName == null) {
            throw new IllegalArgumentException("Invalid column type: null.");
        }
        typeName = typeName.toUpperCase();
        String elementType = Parameters.cleanNull(column.getElementType(), "");
        schema = TypeSchemaMapper.getSchemaForTypeName(typeName, elementType.toUpperCase());
        schema.setAllowNull(column.isNullable());
        schema.setIsKey(column.isPrimary());
        schema.setIndex(index);
        return schema;
    }

    public static DingoSchema createSchemaForColumnDefinition(ColumnDefinition column, int index) {
        DingoSchema schema;
        String typeName = column.getSqlType();
        if (typeName == null) {
            throw new IllegalArgumentException("Invalid column type: null.");
        }
        typeName = typeName.toUpperCase();
        String elementType = Parameters.cleanNull(column.getElementType(), "");
        schema = TypeSchemaMapper.getSchemaForTypeName(typeName, elementType.toUpperCase());
        schema.setAllowNull(column.isNullable());
        schema.setIsKey(column.getIndexOfKey() >= 0);
        schema.setIndex(index);
        return schema;
    }

    public static List<DingoSchema> createSchemaForColumnDefinitions(List<ColumnDefinition> columns) {
        List<ColumnDefinition> orderColumns = sortColumnDefinitions(columns);
        List<DingoSchema> schemas = new ArrayList<>(orderColumns.size());
        for (int i = 0; i < orderColumns.size(); i++) {
            ColumnDefinition column = orderColumns.get(i);
            schemas.add(CodecUtils.createSchemaForColumnDefinition(column, indexOf(column, columns)));
        }
        return schemas;
    }

    public static List<DingoSchema> createSchemaForColumns(List<Column> columns) {
        List<Column> orderColumns = sortColumns(columns);
        List<DingoSchema> schemas = new ArrayList<>(orderColumns.size());
        for (int i = 0; i < orderColumns.size(); i++) {
            Column column = orderColumns.get(i);
            schemas.add(CodecUtils.createSchemaForColumn(column, columns.indexOf(column)));
        }
        return schemas;
    }

    public static DingoSchema createSchemaForTypeName(String typeName) {
        return TypeSchemaMapper.getSchemaForTypeName(typeName, null);
    }

    public static DingoSchema createSchemaForTypeName(String typeName, String elementType) {
        return TypeSchemaMapper.getSchemaForTypeName(typeName, elementType);
    }

    private static int indexOf(ColumnDefinition columnDefinition, List<ColumnDefinition> columns) {
        for (int i = 0; i < columns.size(); i++) {
            if (columns.get(i) == columnDefinition) {
                return i;
            }
        }
        return -1;
    }

    public static byte[] sliceIdKey(byte[] key) {
        return Arrays.copyOf(key, 9);
    }

    public static long readId(byte[] key) {
        BufImpl buf = new BufImpl(key);
        buf.skip(1);
        return buf.readLong();
    }

    public static byte[] encodeId(byte namespace, long id) {
        BufImpl buf = new BufImpl(9);
        buf.write(namespace);
        buf.writeLong(id);
        return buf.getBytes();
    }

}
