package io.dingodb.serial.v2.t1.test;

import io.dingodb.sdk.common.KeyValue;
import io.dingodb.serial.v2.t1.RecordDecoder;
import io.dingodb.serial.v2.t1.RecordEncoder;
import io.dingodb.serial.v2.t1.schema.*;

import java.util.ArrayList;
import java.util.List;

public class TableTest1 {
    public static void main(String[] args) {


        DingoSchema id = new IntegerSchema();
        id.setIndex(0);
        id.setAllowNull(false);
        id.setIsKey(true);

        DingoSchema name = new StringSchema();
        name.setIndex(1);
        name.setAllowNull(false);
        name.setIsKey(true);

        DingoSchema gender = new StringSchema();
        gender.setIndex(2);
        gender.setAllowNull(false);
        gender.setIsKey(true);

        DingoSchema score = new LongSchema();
        score.setIndex(3);
        score.setAllowNull(false);
        score.setIsKey(true);



        DingoSchema addr = new StringSchema();
        addr.setIndex(4);
        addr.setAllowNull(true);
        addr.setIsKey(false);

        DingoSchema exist = new BooleanSchema();
        exist.setIndex(5);
        exist.setAllowNull(false);
        exist.setIsKey(false);

        DingoSchema pic = new BytesSchema();
        pic.setIndex(6);
        pic.setAllowNull(true);
        pic.setIsKey(false);

        DingoSchema testNull = new IntegerSchema();
        testNull.setIndex(7);
        testNull.setAllowNull(true);
        testNull.setIsKey(false);

        DingoSchema age = new IntegerSchema();
        age.setIndex(8);
        age.setAllowNull(false);
        age.setIsKey(false);

        DingoSchema prev = new LongSchema();
        prev.setIndex(9);
        prev.setAllowNull(false);
        prev.setIsKey(false);

        DingoSchema salary = new DoubleSchema();
        salary.setIndex(10);
        salary.setAllowNull(true);
        salary.setIsKey(false);


        List<DingoSchema> table = new ArrayList<>();
        table.add(id);
        table.add(name);
        table.add(gender);
        table.add(score);
        table.add(addr);
        table.add(exist);
        table.add(pic);
        table.add(testNull);
        table.add(age);
        table.add(prev);
        table.add(salary);


        byte[] k1 = new byte[1024];
        for (int i = 0; i < 1024; i++) {
            k1[i] = ((byte) (i%256));
        }



        Object[] record = new Object[] {
                0,
                "tn",
                "f",
                214748364700L,
                "test address test 中文 表情\uD83D\uDE0A\uD83C\uDFF7️\uD83D\uDC4C test 测试测试测试三" +
                        "\uD83E\uDD23\uD83D\uDE02\uD83D\uDE01\uD83D\uDC31\u200D\uD83D\uDC09\uD83D\uDC4F\uD83D\uDC31" +
                        "\u200D\uD83D\uDCBB✔\uD83E\uDD33\uD83E\uDD26\u200D♂️\uD83E\uDD26\u200D♀️\uD83D\uDE4C" +
                        "测试测试测试伍佰肆拾陆万伍仟陆佰伍拾肆元/n/r/r/ndfs肥肉士大夫",
                true,
                k1,
                null,
                -20,
                -214748364700L,
                873485.4234
        };

        List<Object[]> allRecord = new ArrayList<>();
        for (int i = 0; i < 1000000; i ++) {
            Object[] r = new Object[11];
            System.arraycopy(record, 0, r, 0, 11);
            r[0] = i;
            allRecord.add(r);
        }



        RecordEncoder re = new RecordEncoder(0, table);
        RecordDecoder rd = new RecordDecoder(0, table);

        KeyValue kv = re.encode(record);
        System.out.println();
        Object[] result = rd.decode(kv);
        System.out.println();
    }
}
