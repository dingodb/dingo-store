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

import io.dingodb.sdk.common.serial.schema.BooleanSchema;
import io.dingodb.sdk.common.serial.schema.BytesSchema;
import io.dingodb.sdk.common.serial.schema.DingoSchema;
import io.dingodb.sdk.common.serial.schema.DoubleSchema;
import io.dingodb.sdk.common.serial.schema.IntegerSchema;
import io.dingodb.sdk.common.serial.schema.LongSchema;
import io.dingodb.sdk.common.serial.schema.StringSchema;
import io.dingodb.sdk.common.table.Column;

import java.util.ArrayList;
import java.util.List;

public final class CodecUtils {

    private CodecUtils() {
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
        schema = CodecUtils.createSchemaForTypeName(typeName);
        schema.setAllowNull(column.isNullable());
        schema.setIsKey(column.isPrimary());
        schema.setIndex(index);
        return schema;
    }

    public static List<DingoSchema> createSchemaForColumns(List<Column> columns) {
        List<DingoSchema> schemas = new ArrayList<>(columns.size());
        for (int i = 0; i < columns.size(); i++) {
            schemas.add(CodecUtils.createSchemaForColumn(columns.get(i), i));
        }
        return schemas;
    }

    public static DingoSchema createSchemaForTypeName(String typeName) {
        DingoSchema schema;
        switch (typeName) {
            case "INT":
            case "INTEGER":
            case "TINYINT":
                schema = new IntegerSchema();
                break;
            case "LONG":
            case "BIGINT":
                schema =  new LongSchema();
                break;
            case "BOOL":
            case "BOOLEAN":
                schema = new BooleanSchema();
                break;
            case "FLOAT":
            case "DOUBLE":
            case "REAL":
                schema = new DoubleSchema();
                break;
            case "DECIMAL":
            case "STRING":
            case "CHAR":
            case "VARCHAR":
                schema = new StringSchema();
                break;
            case "DATE":
            case "TIME":
            case "TIMESTAMP":
                schema = new LongSchema();
                break;
            case "BINARY":
            case "BYTES":
            case "VARBINARY":
            case "BLOB":
            case "ARRAY":
            case "LIST":
            case "MULTISET":
            case "MAP":
            case "TUPLE":
            case "DICT":
            case "OBJECT":
            case "ANY":
                schema =  new BytesSchema();
                break;
            default:
                throw new IllegalStateException("Unexpected value: " + typeName);
        }
        return schema;
    }

}
