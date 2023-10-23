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

import io.dingodb.sdk.common.DingoCommonId;
import io.dingodb.sdk.common.KeyValue;
import io.dingodb.sdk.common.serial.RecordDecoder;
import io.dingodb.sdk.common.serial.RecordEncoder;
import io.dingodb.sdk.common.serial.schema.DingoSchema;
import io.dingodb.sdk.common.table.Column;
import io.dingodb.sdk.common.table.Table;
import io.dingodb.sdk.common.utils.Optional;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.List;

public class DingoKeyValueCodec implements KeyValueCodec {

    private final long id;
    private final List<DingoSchema> schemas;
    RecordEncoder re;
    RecordDecoder rd;

    public DingoKeyValueCodec(long id, List<DingoSchema> schemas) {
        this(1, id, schemas);
    }

    public DingoKeyValueCodec(int schemaVer, long id, List<DingoSchema> schemas) {
        this.schemas = schemas;
        this.id = id;
        re = new RecordEncoder(schemaVer, schemas, id);
        rd = new RecordDecoder(schemaVer, schemas, id);
    }

    public static DingoKeyValueCodec of(Table table) {
        return of(
            table.getVersion(),
            Optional.mapOrGet(table.id(), DingoCommonId::entityId, () -> 0L),
            table.getColumns()
        );
    }

    public static DingoKeyValueCodec of(long id, Table table) {
        return of(table.getVersion(), id, table);
    }

    public static DingoKeyValueCodec of(long id, List<Column> columns) {
        return of(1, id, columns);
    }

    public static DingoKeyValueCodec of(int schemaVer, long id, Table table) {
        return of(schemaVer, id, table.getColumns());
    }

    public static DingoKeyValueCodec of(int schemaVer, long id, List<Column> columns) {
        return new DingoKeyValueCodec(schemaVer, id, CodecUtils.createSchemaForColumns(columns));
    }

    @Override
    public Object[] decode(KeyValue keyValue) {
        return rd.decode(keyValue);
    }

    @Override
    public Object[] decodeKeyPrefix(byte[] keyPrefix) {
        return rd.decodeKeyPrefix(keyPrefix);
    }

    @Override
    public KeyValue encode(Object @NonNull [] record) {
        return re.encode(record);
    }

    @Override
    public byte[] encodeKey(Object[] record) {
        return re.encodeKey(record);
    }

    @Override
    public byte[] encodeKeyPrefix(Object[] record, int columnCount) {
        return re.encodeKeyPrefix(record, columnCount);
    }

    @Override
    public byte[] encodeMinKeyPrefix() {
        return re.encodeMinKeyPrefix();
    }

    @Override
    public byte[] encodeMaxKeyPrefix() {
        return re.encodeMaxKeyPrefix();
    }

    public byte[] resetPrefix(byte[] key) {
        return re.resetKeyPrefix(key, id);
    }

    @Override
    public byte[] resetPrefix(byte[] key, long prefix) {
        return re.resetKeyPrefix(key, prefix);
    }
}
