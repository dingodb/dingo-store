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
    private final int codecVersion = 0;
    private final int schemaVersion;
    private List<DingoSchema> schemas;
    private final long commonId;
    private int keyBufSize;
    private int valueBufSize;

    public RecordEncoder(int schemaVersion, List<DingoSchema> schemas, long commonId) {
        this.schemaVersion = schemaVersion;
        this.schemas = schemas;
        this.commonId = commonId;
        int[] size = Utils.getApproPerRecordSize(schemas);
        this.keyBufSize = size[0];
        this.valueBufSize = size[1];
    }

    public RecordEncoder(int schemaVersion, long commonId) {
        this.schemaVersion = schemaVersion;
        this.commonId = commonId;
    }


    public KeyValue encode(Object[] record) {
        KeyValue kv = new KeyValue(null, null);
        kv.setKey(encodeKey(record));
        kv.setValue(encodeValue(record));
        return kv;
    }

    public byte[] encodeKey(Object[] record) {
        Buf keyBuf = new BufImpl(keyBufSize);
        keyBuf.writeLong(commonId);
        keyBuf.reverseWriteInt(codecVersion);
        for (DingoSchema schema : schemas) {
            if (schema.isKey()) {
                schema.encodeKey(keyBuf, record[schema.getIndex()]);
            }
        }
        return keyBuf.getBytes();
    }

    public byte[] encodeValue(Object[] record) {
        Buf valueBuf = new BufImpl(valueBufSize);
        valueBuf.writeInt(schemaVersion);
        for (DingoSchema schema : schemas) {
            if (!schema.isKey()) {
                schema.encodeValue(valueBuf, record[schema.getIndex()]);
            }
        }
        return valueBuf.getBytes();
    }

    public byte[] encodeMinKeyPrefix() {
        Buf keyBuf = new BufImpl(8);
        keyBuf.writeLong(commonId);
        return keyBuf.getBytes();
    }

    public byte[] encodeMaxKeyPrefix() {
        if (commonId == Long.MAX_VALUE) {
            throw new RuntimeException("CommonId reach max! Cannot generate Max Key Prefix");
        }
        Buf keyBuf = new BufImpl(8);
        keyBuf.writeLong(commonId+1);
        return keyBuf.getBytes();
    }

    public byte[] encodeKeyPrefix(Object[] record, int columnCount) {
        Buf keyBuf = new BufImpl(keyBufSize);
        keyBuf.writeLong(commonId);
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

    public byte[] updateValueByColumns(byte[] buf, Object[] updateColumn, int[] columnIndexes) {
        return null;
    }
}
