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
import io.dingodb.sdk.common.type.DingoType;
import io.dingodb.sdk.common.type.TupleMapping;
import io.dingodb.sdk.common.type.converter.DingoConverter;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.io.IOException;
import java.util.List;

public class DingoKeyValueCodec implements KeyValueCodec {

    private DingoType type;
    private List<DingoSchema> table;
    RecordEncoder re;
    RecordDecoder rd;

    public DingoKeyValueCodec(DingoType type, TupleMapping keyMapping, long commonId) {
        this.type = type;
        table = type.toDingoSchemas();
        for (DingoSchema schema : table) {
            if (keyMapping.contains(schema.getIndex())) {
                schema.setIsKey(true);
            } else {
                schema.setIsKey(false);
            }
        }
        re = new RecordEncoder(0, table, commonId);
        rd = new RecordDecoder(0, table, commonId);
    }

    @Override
    public Object[] decode(KeyValue keyValue) throws IOException {
        return (Object[]) type.convertFrom(rd.decode(keyValue), DingoConverter.INSTANCE);
    }

    @Override
    public Object[] decodeKeyPrefix(byte[] keyPrefix) throws IOException {
        return (Object[]) type.convertFrom(rd.decodeKeyPrefix(keyPrefix), DingoConverter.INSTANCE);
    }

    @Override
    public KeyValue encode(Object @NonNull [] record) throws IOException {
        Object[] converted = (Object[]) type.convertTo(record, DingoConverter.INSTANCE);
        return re.encode(converted);
    }

    @Override
    public byte[] encodeKey(Object[] record) throws IOException {
        Object[] converted = (Object[]) type.convertTo(record, DingoConverter.INSTANCE);
        return re.encodeKey(converted);
    }

    @Override
    public byte[] encodeKeyPrefix(Object[] record, int columnCount) throws IOException {
        Object[] converted = (Object[]) type.convertTo(record, DingoConverter.INSTANCE);
        return re.encodeKeyPrefix(converted, columnCount);
    }

    @Override
    public byte[] encodeMinKeyPrefix() throws IOException {
        return re.encodeMinKeyPrefix();
    }

    @Override
    public byte[] encodeMaxKeyPrefix() throws IOException {
        return re.encodeMaxKeyPrefix();
    }
}
