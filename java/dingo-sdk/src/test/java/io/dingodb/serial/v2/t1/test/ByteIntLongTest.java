package io.dingodb.serial.v2.t1.test;

import io.dingodb.sdk.common.KeyValue;
import io.dingodb.serial.v2.t1.RecordDecoder;
import io.dingodb.serial.v2.t1.RecordEncoder;
import io.dingodb.serial.v2.t1.schema.BytesSchema;
import io.dingodb.serial.v2.t1.schema.DingoSchema;
import io.dingodb.serial.v2.t1.schema.IntegerSchema;
import io.dingodb.serial.v2.t1.schema.LongSchema;

import java.util.ArrayList;
import java.util.List;

public class ByteIntLongTest {
    public static void main(String[] args) {
        DingoSchema id = new IntegerSchema();
        id.setIndex(0);
        id.setAllowNull(false);
        id.setIsKey(true);

        DingoSchema name = new BytesSchema();
        name.setIndex(1);
        name.setAllowNull(false);
        name.setIsKey(true);

        DingoSchema gender = new BytesSchema();
        gender.setIndex(2);
        gender.setAllowNull(false);
        gender.setIsKey(true);

        DingoSchema score = new LongSchema();
        score.setIndex(3);
        score.setAllowNull(false);
        score.setIsKey(true);



        DingoSchema addr = new BytesSchema();
        addr.setIndex(4);
        addr.setAllowNull(true);
        addr.setIsKey(false);

        DingoSchema exist = new BytesSchema();
        exist.setIndex(5);
        exist.setAllowNull(false);
        exist.setIsKey(false);

        DingoSchema pic = new BytesSchema();
        pic.setIndex(6);
        pic.setAllowNull(true);
        pic.setIsKey(false);

        DingoSchema age = new IntegerSchema();
        age.setIndex(7);
        age.setAllowNull(false);
        age.setIsKey(false);

        DingoSchema prev = new LongSchema();
        prev.setIndex(8);
        prev.setAllowNull(false);
        prev.setIsKey(false);


        List<DingoSchema> table = new ArrayList<>();
        table.add(id);
        table.add(name);
        table.add(gender);
        table.add(score);
        table.add(addr);
        table.add(exist);
        table.add(pic);
        table.add(age);
        table.add(prev);

        Object[] record1 = new Object[]{0, new byte[] {0}, new byte[]{0,0,0,0,0,0,0,0,0,0,0,0,0}, 0L, new byte[]{0,0,0,0}, new byte[] {0,0,0,0,0}, null, 0, 0L};


        RecordEncoder re = new RecordEncoder(0, table);
        KeyValue keyValue = re.encode(record1);
        System.out.println(keyValue);

        RecordDecoder rd = new RecordDecoder(0, table);
        Object result = rd.decode(keyValue);
        System.out.println(result);
    }
}
