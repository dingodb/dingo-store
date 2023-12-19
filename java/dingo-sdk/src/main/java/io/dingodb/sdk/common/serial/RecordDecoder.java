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
    private final int schemaVersion;
    private List<DingoSchema> schemas;
    private final long id;

    public RecordDecoder(int schemaVersion, List<DingoSchema> schemas, long id) {
        this.schemaVersion = schemaVersion;
        this.schemas = schemas;
        this.id = id;
    }

    private void checkPrefix(Buf buf) {
        buf.skip(1);

        long keyId = buf.readLong();
        if (keyId != id) {
            throw new RuntimeException("Invalid prefix id, codec prefix id: " + id + ", key prefix id " + keyId);
        }
    }

    private void checkTag(Buf buf) {
        int codecVer = buf.reverseRead();
        if (codecVer > Config.CODEC_VERSION) {
            throw new RuntimeException(
                "Invalid codec version, codec version: " + Config.CODEC_VERSION + ", key codec version " + codecVer
            );
        }

        buf.reverseSkip(3);
    }

    private void checkSchemaVersion(Buf buf) {
        int schemaVer = buf.readInt();
        if (schemaVer > this.schemaVersion) {
            throw new RuntimeException(
                "Invalid schema version, schema version: " + this.schemaVersion + ", key schema version " + schemaVer
            );
        }
    }

    private void checkKeyValue(Buf keyBuf, Buf valueBuf) {
        checkTag(keyBuf);
        checkPrefix(keyBuf);
        checkSchemaVersion(valueBuf);
    }

    public Object[] decode(KeyValue keyValue) {
        Buf keyBuf = new BufImpl(keyValue.getKey());
        Buf valueBuf = new BufImpl(keyValue.getValue());

        checkKeyValue(keyBuf, valueBuf);

        Object[] record = new Object[schemas.size()];
        for (DingoSchema<?> schema : schemas) {
            if (schema.isKey()) {
                record[schema.getIndex()] = schema.decodeKey(keyBuf);
            } else {
                if (valueBuf.isEnd()) {
                    continue;
                }
                record[schema.getIndex()] = schema.decodeValue(valueBuf);
            }
        }
        return record;
    }

    public Object[] decode(KeyValue keyValue, int[] columnIndexes) {
        Buf keyBuf = new BufImpl(keyValue.getKey());
        Buf valueBuf = new BufImpl(keyValue.getValue());

        checkKeyValue(keyBuf, valueBuf);

        Object[] record = new Object[columnIndexes.length];
        int i = 0;
        for (DingoSchema<?> schema : schemas) {
            if (Arrays.binarySearch(columnIndexes, schema.getIndex()) < 0) {
                if (schema.isKey()) {
                    schema.skipKey(keyBuf);
                } else if (!valueBuf.isEnd()) {
                    schema.skipValue(valueBuf);
                }
            } else {
                if (schema.isKey()) {
                    record[i] = schema.decodeKey(keyBuf);
                } else if (!valueBuf.isEnd()) {
                    record[i] = schema.decodeValue(valueBuf);
                }
                i++;
            }
        }
        return record;
    }

    public Object[] decodeKeyPrefix(byte[] keyPrefix) {
        Buf keyPrefixBuf = new BufImpl(keyPrefix);

        keyPrefixBuf.skip(1);

        if (keyPrefixBuf.readLong() != id) {
            throw new RuntimeException("Wrong Common Id");
        }
        Object[] record = new Object[schemas.size()];
        for (DingoSchema<?> schema : schemas) {
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
        if (valueBuf.readInt() > schemaVersion) {
            throw new RuntimeException("Wrong Schema Version");
        }
        Object[] record = new Object[schemas.size()];
        for (DingoSchema<?> schema : schemas) {
            if (valueBuf.isEnd()) {
                break;
            }
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
