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

package io.dingodb.sdk.common.serial;

import io.dingodb.sdk.common.KeyValue;
import io.dingodb.sdk.common.serial.schema.DingoSchema;

import java.util.Arrays;
import java.util.List;

public class RecordDecoder {
    private final int codecVersion = 0;
    private final int schemaVersion;
    private List<DingoSchema> schemas;
    private final long commonId;

    public RecordDecoder(int schemaVersion, List<DingoSchema> schemas, long commonId) {
        this.schemaVersion = schemaVersion;
        this.schemas = schemas;
        this.commonId = commonId;
    }

    public Object[] decode(KeyValue keyValue) {
        Buf keyBuf = new BufImpl(keyValue.getKey());
        Buf valueBuf = new BufImpl(keyValue.getValue());
        if (keyBuf.readLong() != commonId) {
            throw new RuntimeException("Wrong Common Id");
        }
        if (keyBuf.reverseReadInt() != codecVersion) {
            throw new RuntimeException("Wrong Codec Version");
        }
        if (valueBuf.readInt() != schemaVersion) {
            throw new RuntimeException("Wrong Schema Version");
        }
        Object[] record = new Object[schemas.size()];
        for (DingoSchema schema : schemas) {
            if (schema.isKey()) {
                record[schema.getIndex()] = schema.decodeKey(keyBuf);
            } else {
                record[schema.getIndex()] = schema.decodeValue(valueBuf);
            }
        }
        return record;
    }

    public Object[] decode(KeyValue keyValue, int[] columnIndexes) {
        Buf keyBuf = new BufImpl(keyValue.getKey());
        Buf valueBuf = new BufImpl(keyValue.getValue());
        if (keyBuf.readLong() != commonId) {
            throw new RuntimeException("Wrong Common Id");
        }
        if (keyBuf.reverseReadInt() != codecVersion) {
            throw new RuntimeException("Wrong Codec Version");
        }
        if (valueBuf.readInt() != schemaVersion) {
            throw new RuntimeException("Wrong Schema Version");
        }
        Object[] record = new Object[schemas.size()];
        for (DingoSchema schema : schemas) {
            if (Arrays.binarySearch(columnIndexes, schema.getIndex()) < 0) {
                if (schema.isKey()) {
                    schema.skipKey(keyBuf);
                } else {
                    schema.skipValue(valueBuf);
                }
            } else {
                if (schema.isKey()) {
                    record[schema.getIndex()] = schema.decodeKey(keyBuf);
                } else {
                    record[schema.getIndex()] = schema.decodeValue(valueBuf);
                }
            }
        }
        return record;
    }

    public Object[] decodeKeyPrefix(byte[] keyPrefix) {
        Buf keyPrefixBuf = new BufImpl(keyPrefix);
        if (keyPrefixBuf.readLong() != commonId) {
            throw new RuntimeException("Wrong Common Id");
        }
        Object[] record = new Object[schemas.size()];
        for (DingoSchema schema : schemas) {
            if (keyPrefixBuf.isEnd()) {
                break;
            }
            if (schema.isKey()) {
                record[schema.getIndex()] = schema.decodeKeyPrefix(keyPrefixBuf);
            }
        }
        return record;
    }

    public Object[] decodeValue(KeyValue keyValue, int[] columnIndexes) {
        Buf valueBuf = new BufImpl(keyValue.getValue());
        if (valueBuf.readInt() != schemaVersion) {
            throw new RuntimeException("Wrong Schema Version");
        }
        Object[] record = new Object[schemas.size()];
        for (DingoSchema schema : schemas) {
            if (!schema.isKey()) {
                if (Arrays.binarySearch(columnIndexes, schema.getIndex()) < 0) {
                    schema.skipValue(valueBuf);
                } else {
                    record[schema.getIndex()] = schema.decodeValue(valueBuf);
                }
            }
        }
        return record;
    }
}
