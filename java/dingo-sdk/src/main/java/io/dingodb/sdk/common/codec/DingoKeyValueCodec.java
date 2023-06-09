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

import io.dingodb.sdk.common.KeyValue;
import io.dingodb.sdk.common.serial.RecordDecoder;
import io.dingodb.sdk.common.serial.RecordEncoder;
import io.dingodb.sdk.common.serial.schema.DingoSchema;
import io.dingodb.sdk.common.table.Column;
import io.dingodb.sdk.common.table.Table;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class DingoKeyValueCodec implements KeyValueCodec {

    private List<DingoSchema> schemas;
    RecordEncoder re;
    RecordDecoder rd;

    public DingoKeyValueCodec(long commonId, List<DingoSchema> schemas) {
        this.schemas = schemas;
        re = new RecordEncoder(0, schemas, commonId);
        rd = new RecordDecoder(0, schemas, commonId);
    }

    public static DingoKeyValueCodec of(long id, Table table) {
        return of(id, table.getColumns());
    }

    public static DingoKeyValueCodec of(long id, List<Column> columns) {
        return new DingoKeyValueCodec(id, CodecUtils.createSchemaForColumns(columns));
    }

    @Override
    public Object[] decode(KeyValue keyValue) throws IOException {
        return rd.decode(keyValue);
    }

    @Override
    public Object[] decodeKeyPrefix(byte[] keyPrefix) throws IOException {
        return rd.decodeKeyPrefix(keyPrefix);
    }

    @Override
    public KeyValue encode(Object @NonNull [] record) throws IOException {
        return re.encode(record);
    }

    @Override
    public byte[] encodeKey(Object[] record) throws IOException {
        return re.encodeKey(record);
    }

    @Override
    public byte[] encodeKeyPrefix(Object[] record, int columnCount) throws IOException {
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
}
