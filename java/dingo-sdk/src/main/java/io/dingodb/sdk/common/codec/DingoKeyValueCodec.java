package io.dingodb.sdk.common.codec;

import io.dingodb.sdk.common.KeyValue;
import io.dingodb.sdk.common.type.DingoType;
import io.dingodb.sdk.common.type.TupleMapping;
import io.dingodb.sdk.common.type.converter.DingoConverter;
import io.dingodb.serial.v2.t1.RecordDecoder;
import io.dingodb.serial.v2.t1.RecordEncoder;
import io.dingodb.serial.v2.t1.schema.DingoSchema;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.io.IOException;
import java.util.List;

public class DingoKeyValueCodec implements KeyValueCodec {

    private DingoType type;
    private List<DingoSchema> table;
    RecordEncoder re;
    RecordDecoder rd;

    public DingoKeyValueCodec(DingoType type, TupleMapping keyMapping) {
        this.type = type;
        table = type.toDingoSchemas();
        for (DingoSchema schema : table) {
            if (keyMapping.contains(schema.getIndex())) {
                schema.setIsKey(true);
            } else {
                schema.setIsKey(false);
            }
        }
        re = new RecordEncoder(0, table);
        rd = new RecordDecoder(0, table);
    }

    @Override
    public Object[] decode(KeyValue keyValue) throws IOException {
        return (Object[]) type.convertFrom(rd.decode(keyValue), DingoConverter.INSTANCE);
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
}
