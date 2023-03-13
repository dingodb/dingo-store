package io.dingodb.serial.v1.test;

import io.dingodb.serial.v1.io.RecordDecoder;
import io.dingodb.serial.v1.schema.*;
import io.dingodb.serial.v1.io.RecordEncoder;
import io.dingodb.serial.v2.KeyValue;
import jdk.nashorn.internal.ir.debug.ObjectSizeCalculator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class FinalTest {

    public static void main(String[] args) throws IOException {

        List<List<DingoSchema>> table = getTable();
        RecordEncoder reKey =
                new RecordEncoder(table.get(0), (short) 0, (byte) 0, (byte) 0, (byte) 0, null);
        RecordEncoder reValue =
                new RecordEncoder(table.get(1), (short) 0, (byte) 0, (byte) 0, (byte) 0, null);

        List<Object[]> records = getRecords();


        long tag1 = System.currentTimeMillis();
        for (Object[] record : records) {
            byte[] key = reKey.encodeKey(record);
            byte[] value = reValue.encode(record);
            KeyValue kv = new KeyValue();
            kv.setKey(key);
            kv.setValue(value);
        }
        long tag2 = System.currentTimeMillis();


        List<KeyValue> kvs = new ArrayList<>();
        for (Object[] record : records) {
            byte[] key = reKey.encodeKey(record);
            byte[] value = reValue.encode(record);
            KeyValue kv = new KeyValue();
            kv.setKey(key);
            kv.setValue(value);
            kvs.add(kv);
        }


        RecordDecoder rdKey =
                new RecordDecoder(table.get(0), (short) 0, (byte) 0, (byte) 0, (byte) 0, null, true);

        RecordDecoder rdValue =
                new RecordDecoder(table.get(1), (short) 0, (byte) 0, (byte) 0, (byte) 0, null, false);

        int[] index = new int[]{4,5,10};
        long tag3 = System.currentTimeMillis();
        for (KeyValue kv : kvs) {
            Object[] result = rdValue.decode(kv.getValue(), index);
            result[0] = result[0] + "a";
            result[1] = false;
            result[2] = (double)result[2] + 1;
            kv.setValue(reValue.encode(kv.getValue(), index, result));
        }
        long tag4 = System.currentTimeMillis();


        long tag5 = System.currentTimeMillis();
        for (KeyValue kv : kvs) {
            Object[] record1 = rdValue.decode(kv.getValue());
        }
        long tag6 = System.currentTimeMillis();

        System.out.println(ObjectSizeCalculator.getObjectSize(kvs));
        System.out.println("Stage1 : " + (tag2 - tag1));
        System.out.println("Stage2 : " + (tag4 - tag3));
        System.out.println("Stage3 : " + (tag6 - tag5));
    }


    private static List<List<DingoSchema>> getTable() {
        DingoSchema id = new IntegerSchema(0);
        id.setNotNull(true);

        DingoSchema name = new StringSchema(1, 0);
        name.setNotNull(true);

        DingoSchema gender = new StringSchema(2, 0);
        gender.setNotNull(true);

        DingoSchema score = new LongSchema(3);
        score.setNotNull(true);

        DingoSchema addr = new StringSchema(4, 0);

        DingoSchema exist = new BooleanSchema(5);
        exist.setNotNull(true);

        DingoSchema pic = new BytesSchema(6);
        pic.setNotNull(false);

        DingoSchema testNull = new IntegerSchema(7);
        testNull.setNotNull(false);

        DingoSchema age = new IntegerSchema(8);
        age.setNotNull(true);

        DingoSchema prev = new LongSchema(9);
        prev.setNotNull(true);

        DingoSchema salary = new DoubleSchema(10);
        salary.setNotNull(false);

        List<DingoSchema> tableKey = new ArrayList<>();
        List<DingoSchema> tableValue = new ArrayList<>();
        tableKey.add(id);
        tableKey.add(name);
        tableKey.add(gender);
        tableKey.add(score);

        tableValue.add(id);
        tableValue.add(name);
        tableValue.add(gender);
        tableValue.add(score);
        tableValue.add(addr);
        tableValue.add(exist);
        tableValue.add(pic);
        tableValue.add(testNull);
        tableValue.add(age);
        tableValue.add(prev);
        tableValue.add(salary);

        List<List<DingoSchema>> table = new ArrayList<>();
        table.add(tableKey);
        table.add(tableValue);

        return table;
    }

    private static List<Object[]> getRecords() {
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
            System.arraycopy(record, 1, r, 1, 10);
            r[0] = i;
            allRecord.add(r);
        }

        return allRecord;
    }
}
