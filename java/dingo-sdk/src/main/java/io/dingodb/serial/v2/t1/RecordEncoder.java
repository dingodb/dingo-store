package io.dingodb.serial.v2.t1;

import io.dingodb.sdk.common.KeyValue;
import io.dingodb.serial.v2.t1.schema.DingoSchema;

import java.util.Arrays;
import java.util.List;

public class RecordEncoder {
    private final int codecVersion = 0;
    private final int schemaVersion;
    private List<DingoSchema> schemas;
    private int keyBufSize;
    private int valueBufSize;

    public RecordEncoder(int schemaVersion, List<DingoSchema> schemas) {
        this.schemaVersion = schemaVersion;
        this.schemas = schemas;
        Utils.sortSchema(this.schemas);
        int[] size = Utils.getApproPerRecordSize(schemas);
        this.keyBufSize = size[0];
        this.valueBufSize = size[1];
    }


    public KeyValue encode(Object[] record) {
        KeyValue kv = new KeyValue(null, null);
        kv.setKey(encodeKey(record));
        kv.setValue(encodeValue(record));
        return kv;
    }

    public byte[] encodeKey(Object[] record) {
        Buf keyBuf = new BufImpl(keyBufSize);
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

    public byte[] encodeKeyPrefix(Object[] record) {
        return null;
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
