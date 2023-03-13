package io.dingodb.serial.v2.t2;

import io.dingodb.sdk.common.KeyValue;
import io.dingodb.serial.v2.t2.schema.DingoSchema;

import java.util.Arrays;
import java.util.List;

public class RecordDecoder {

    private int codecVersion = 0;
    private int schemaVersion;
    List<DingoSchema> schemas;

    public RecordDecoder(int schemaVersion, List<DingoSchema> schemas) {
        this.schemaVersion = schemaVersion;
        this.schemas = schemas;
    }


    public Object[] decode(List<KeyValue> kvs) {
        Buf keyBuf = new Buf(kvs.get(0).getKey());
        ValueBuf valueBuf = new ValueBuf(kvs.get(0).getValue());
        if (keyBuf.readKeyInt() != codecVersion) {
            throw new RuntimeException("Wrong Codec Version");
        }
        if (valueBuf.readInt() != schemaVersion) {
            throw new RuntimeException("Wrong Schema Version");
        }
        int index = 1;
        Object[] record = new Object[schemas.size()];
        for (DingoSchema schema : schemas) {
            if (schema.isKey()) {
                record[schema.getIndex()] = schema.decodeKey(keyBuf);
            } else if (schema.getLength() != 0) {
                record[schema.getIndex()] = schema.decodeValue(valueBuf);
            } else {
                ValueBuf vb = new ValueBuf(kvs.get(index++).getValue());
                if (vb.readInt() != schemaVersion) {
                    throw new RuntimeException("Wrong Schema Version");
                }
                record[schema.getIndex()] = schema.decodeValue(vb);
            }
        }
        return record;
    }

    public Object[] decode(List<KeyValue> kvs, int[] column) {
        Buf keyBuf = new Buf(kvs.get(0).getKey());
        ValueBuf valueBuf = new ValueBuf(kvs.get(0).getValue());
        if (keyBuf.readKeyInt() != codecVersion) {
            throw new RuntimeException("Wrong Codec Version");
        }
        if (valueBuf.readInt() != schemaVersion) {
            throw new RuntimeException("Wrong Schema Version");
        }
        int index = 1;
        Object[] record = new Object[schemas.size()];
        for (DingoSchema schema : schemas) {
            if (Arrays.binarySearch(column, schema.getIndex()) >= 0) {
                if (schema.isKey()) {
                    record[schema.getIndex()] = schema.decodeKey(keyBuf);
                } else if (schema.getLength() != 0) {
                    record[schema.getIndex()] = schema.decodeValue(valueBuf);
                } else {
                    ValueBuf vb = new ValueBuf(kvs.get(index++).getValue());
                    if (vb.readInt() != schemaVersion) {
                        throw new RuntimeException("Wrong Schema Version");
                    }
                    record[schema.getIndex()] = schema.decodeValue(vb);
                }
            }
            else {
                if (schema.isKey()) {
                    schema.skipKey(keyBuf);
                } else if (schema.getLength() != 0) {
                    schema.skipValue(valueBuf);
                }
            }
        }
        return record;
    }

}
