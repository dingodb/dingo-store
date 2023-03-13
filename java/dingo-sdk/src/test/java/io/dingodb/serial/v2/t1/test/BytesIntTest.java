package io.dingodb.serial.v2.t1.test;

import io.dingodb.sdk.common.KeyValue;
import io.dingodb.serial.v2.t1.RecordDecoder;
import io.dingodb.serial.v2.t1.RecordEncoder;
import io.dingodb.serial.v2.t1.schema.BytesSchema;
import io.dingodb.serial.v2.t1.schema.DingoSchema;
import io.dingodb.serial.v2.t1.schema.IntegerSchema;

import java.util.ArrayList;
import java.util.List;

public class BytesIntTest {


    public static void main(String[] args) {

        DingoSchema i1 = new IntegerSchema();
        i1.setIndex(0);
        i1.setAllowNull(false);
        i1.setIsKey(true);

        DingoSchema b1 = new BytesSchema();
        b1.setIndex(1);
        b1.setAllowNull(false);
        b1.setIsKey(true);

        DingoSchema i2 = new IntegerSchema();
        i2.setIndex(2);
        i2.setAllowNull(true);
        i2.setIsKey(true);

        DingoSchema b2 = new BytesSchema();
        b2.setIndex(3);
        b2.setAllowNull(true);
        b2.setIsKey(true);

        List<DingoSchema> schemas = new ArrayList<>();
        schemas.add(i1);
        schemas.add(b1);
        schemas.add(i2);
        schemas.add(b2);


        RecordEncoder re = new RecordEncoder(0, schemas);

        Object[] record1 = new Object[] {1, new byte[]{1,2,3}, 2, new byte[]{4,5,6}};

        KeyValue kv = re.encode(record1);

        System.out.println(kv);

        RecordDecoder rd = new RecordDecoder(0, schemas);
        Object[] record_1 = rd.decode(kv);

        System.out.println(record_1);

    }

}
