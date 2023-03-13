package io.dingodb.serial.v2.t2;

import io.dingodb.sdk.common.KeyValue;
import io.dingodb.serial.v2.t2.schema.DingoSchema;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class RecordEncoder {
    private final int codecVersion = 0;
    private final int schemaVersion;
    private List<DingoSchema> schemas;
    private int bufSize;
    private Map<Integer, Integer> valueBufSize = new TreeMap<>();

    public RecordEncoder(int schemaVersion, List<DingoSchema> schemas) {
        this.schemaVersion = schemaVersion;
        this.schemas = schemas;
        int[] size = Utils.getApproPerRecordSize(schemas);
        this.bufSize = size[0];
        for (int i = 1; i < size.length; i++) {
            if (size[i] != 0) {
                valueBufSize.put(i-1, size[i]);
            }
        }
    }


    public List<KeyValue> encode(Object[] record) {
        Buf buf = new Buf(bufSize, valueBufSize);
        buf.writeKeyInt(schemaVersion);
        buf.writeValueInt(schemaVersion, 0);
        for (DingoSchema schema : schemas) {
            if (schema.isKey()) {
                schema.encodeKey(buf, record[schema.getIndex()]);
            } else {
                if (schema.getLength() == 0) {
                    buf.writeValueInt(schemaVersion, schema.getIndex());
                }
                schema.encodeValue(buf, record[schema.getIndex()]);
            }
        }
        return buf.getData();
    }

    public List<byte[]> encodeKey(Object[] record) {
        Buf buf = new Buf(bufSize, valueBufSize);
        for (DingoSchema schema : schemas) {
            if (schema.isKey()) {
                schema.encodeKey(buf, record[schema.getIndex()]);
            }
        }
        return buf.getKeyData();
    }

    public byte[] encodeKeyPrefix(Object[] record) {
        return null;
    }

    public void updateValueByRecord(List<KeyValue> kvs, Object[] record, int[] columnIndexes) {
        ValueBuf valueBuf = new ValueBuf(kvs.get(0).getValue());
        if (valueBuf.readInt() != schemaVersion) {
            throw new RuntimeException("Wrong Schema Version");
        }
        int index = 1;
        for (DingoSchema schema : schemas) {
            if (!schema.isKey()) {
                if (schema.getLength() != 0) {
                    if (Arrays.binarySearch(columnIndexes, schema.getIndex()) < 0) {
                        valueBuf.skip(schema.getLength());
                    } else {
                        schema.encodeValue(valueBuf, record[schema.getIndex()]);
                    }
                } else {
                    if (Arrays.binarySearch(columnIndexes, schema.getIndex()) >= 0) {
                        ValueBuf buf = new ValueBuf(kvs.get(index).getValue());
                        if (buf.readInt() != schemaVersion) {
                            throw new RuntimeException("Wrong Schema Version");
                        }
                        schema.encodeValue(buf, record[schema.getIndex()]);
                        kvs.get(index++).setValue(buf.getBytes());
                    }
                }
            }
        }

        kvs.get(0).setValue(valueBuf.getBytes());
    }
}
