package io.dingodb.serial.v2.t1;

import io.dingodb.sdk.common.KeyValue;
import io.dingodb.serial.v2.t1.schema.DingoSchema;

import java.util.Arrays;
import java.util.List;

public class RecordDecoder {
    private final int codecVersion = 0;
    private final int schemaVersion;
    private List<DingoSchema> schemas;

    public RecordDecoder(int schemaVersion, List<DingoSchema> schemas) {
        this.schemaVersion = schemaVersion;
        this.schemas = schemas;
        Utils.sortSchema(this.schemas);
    }

    public Object[] decode(KeyValue keyValue) {
        Buf keyBuf = new BufImpl(keyValue.getKey());
        Buf valueBuf = new BufImpl(keyValue.getValue());
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
