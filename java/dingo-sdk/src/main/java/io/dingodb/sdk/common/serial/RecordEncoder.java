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

public class RecordEncoder {
    private final int schemaVersion;
    private List<DingoSchema> schemas;
    private final long id;
    private int keyBufSize;
    private int valueBufSize;

    public RecordEncoder(int schemaVersion, List<DingoSchema> schemas, long id) {
        this.schemaVersion = schemaVersion;
        this.schemas = schemas;
        this.id = id;
        int[] size = Utils.getApproPerRecordSize(schemas);
        this.keyBufSize = size[0];
        this.valueBufSize = size[1];
    }

    public RecordEncoder(int schemaVersion, long id) {
        this.schemaVersion = schemaVersion;
        this.id = id;
    }

    private void encodePrefix(Buf buf) {
        buf.write((byte) 'r');
        buf.writeLong(id);
    }

    private void encodeTag(Buf buf) {
        buf.reverseWrite(Config.CODEC_VERSION);
        buf.reverseWrite((byte) 0);
        buf.reverseWrite((byte) 0);
        buf.reverseWrite((byte) 0);
    }

    private void encodeSchemaVersion(Buf buf) {
        buf.writeInt(schemaVersion);
    }

    public KeyValue encode(Object[] record) {
        KeyValue kv = new KeyValue(null, null);
        kv.setKey(encodeKey(record));
        kv.setValue(encodeValue(record));
        return kv;
    }

    public byte[] encodeKey(Object[] record) {
        Buf keyBuf = new BufImpl(keyBufSize);

        encodeTag(keyBuf);
        encodePrefix(keyBuf);

        for (DingoSchema schema : schemas) {
            if (schema.isKey()) {
                schema.encodeKey(keyBuf, record[schema.getIndex()]);
            }
        }
        return keyBuf.getBytes();
    }

    public byte[] encodeValue(Object[] record) {
        Buf valueBuf = new BufImpl(valueBufSize);
        encodeSchemaVersion(valueBuf);
        for (DingoSchema schema : schemas) {
            if (!schema.isKey()) {
                schema.encodeValue(valueBuf, record[schema.getIndex()]);
            }
        }
        return valueBuf.getBytes();
    }

    public byte[] encodeMinKeyPrefix() {
        Buf keyBuf = new BufImpl(9);
        encodePrefix(keyBuf);
        return keyBuf.getBytes();
    }

    public byte[] encodeMaxKeyPrefix() {
        if (id == Long.MAX_VALUE) {
            throw new RuntimeException("CommonId reach max! Cannot generate Max Key Prefix");
        }
        Buf keyBuf = new BufImpl(9);
        keyBuf.write((byte) 'r');
        keyBuf.writeLong(id + 1);
        return keyBuf.getBytes();
    }

    public byte[] encodeKeyPrefix(Object[] record, int columnCount) {
        Buf keyBuf = new BufImpl(keyBufSize);
        encodePrefix(keyBuf);
        for (DingoSchema schema : schemas) {
            if (schema.isKey()) {
                if (columnCount-- > 0) {
                    schema.encodeKeyPrefix(keyBuf, record[schema.getIndex()]);
                } else {
                    break;
                }
            }
        }
        return keyBuf.getBytes();
    }

    public byte[] updateValueByRecord(byte[] buf, Object[] record, int[] columnIndexes) {
        Buf valueBuf = new BufImpl(buf);
        if (valueBuf.readInt() != schemaVersion) {
            throw new RuntimeException("Wrong Schema Version");
        }
        for (DingoSchema schema : schemas) {
            if (!schema.isKey()) {
                if (Arrays.binarySearch(columnIndexes, schema.getIndex()) < 0) {
                    schema.skipValue(valueBuf);
                } else {
                    schema.encodeValue(valueBuf, record[schema.getIndex()]);
                }
            }
        }
        return valueBuf.getBytes();
    }

    public byte[] resetKeyPrefix(byte[] key, long prefix) {
        BufImpl buf = new BufImpl(key);
        buf.skip(1);
        buf.writeLong(prefix);
        return key;
    }

    public byte[] updateValueByColumns(byte[] buf, Object[] updateColumn, int[] columnIndexes) {
        return null;
    }
}
