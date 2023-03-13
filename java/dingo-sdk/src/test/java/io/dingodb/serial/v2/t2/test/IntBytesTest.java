package io.dingodb.serial.v2.t2.test;

import io.dingodb.sdk.common.KeyValue;
import io.dingodb.serial.v2.t2.RecordDecoder;
import io.dingodb.serial.v2.t2.RecordEncoder;
import io.dingodb.serial.v2.t2.schema.BytesSchema;
import io.dingodb.serial.v2.t2.schema.DingoSchema;
import io.dingodb.serial.v2.t2.schema.IntegerSchema;

import java.util.ArrayList;
import java.util.List;

public class IntBytesTest {
    public static void main(String[] args) {

        List<DingoSchema> schemas = new ArrayList<>();

        IntegerSchema integerSchema = new IntegerSchema();
        integerSchema.setIndex(0);
        integerSchema.setAllowNull(true);
        integerSchema.setIsKey(true);
        schemas.add(integerSchema);

        BytesSchema bytesSchema = new BytesSchema();
        bytesSchema.setIndex(1);
        bytesSchema.setAllowNull(true);
        bytesSchema.setIsKey(true);
        schemas.add(bytesSchema);

        IntegerSchema integerSchema1 = new IntegerSchema();
        integerSchema1.setIndex(2);
        integerSchema1.setAllowNull(true);
        integerSchema1.setIsKey(false);
        schemas.add(integerSchema1);

        BytesSchema bytesSchema1 = new BytesSchema();
        bytesSchema1.setIndex(3);
        bytesSchema1.setAllowNull(true);
        bytesSchema1.setIsKey(false);
        schemas.add(bytesSchema1);


        RecordEncoder re = new RecordEncoder(0, schemas);

        List<KeyValue> kvs = re.encode(new Object[]{1, new byte[]{(byte) 1, (byte) 2}, 1, new byte[]{(byte) 1, (byte) 2}});

        System.out.println(kvs);

        RecordDecoder rd = new RecordDecoder(0, schemas);
        Object[] record = rd.decode(kvs);

        record[2] = 2;
        record[3] = new byte[]{(byte) 3, (byte) 4};

        re.updateValueByRecord(kvs, record, new int[] {2,3});

        record = rd.decode(kvs);
        System.out.println(record);
    }
}
