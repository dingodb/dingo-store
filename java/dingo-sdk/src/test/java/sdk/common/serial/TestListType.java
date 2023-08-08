/*
 * Copyright 2021 DataCanvas
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package sdk.common.serial;

import io.dingodb.sdk.common.KeyValue;
import io.dingodb.sdk.common.serial.RecordDecoder;
import io.dingodb.sdk.common.serial.RecordEncoder;
import io.dingodb.sdk.common.serial.schema.*;
import jdk.nashorn.internal.ir.debug.ObjectSizeCalculator;

import java.util.ArrayList;
import java.util.List;

public class TestListType {

    public static void main(String[] args) {
        List<DingoSchema> table = getTable();
        RecordEncoder re = new RecordEncoder(0, table, 0L);
        RecordDecoder rd = new RecordDecoder(0, table, 0L);

        List<Object[]> records = getRecords();

        long tag1 = System.currentTimeMillis();
        for(Object[] record : records) {
            KeyValue kv = re.encode(record);
        }
        long tag2 = System.currentTimeMillis();

        List<KeyValue> kvs = new ArrayList<>();
        for(Object[] record : records) {
            KeyValue kv = re.encode(record);
            kvs.add(kv);
        }

        long tag5 = System.currentTimeMillis();
        for(KeyValue kv : kvs) {
            rd.decode(kv);
        }
        long tag6 = System.currentTimeMillis();

        System.out.println(ObjectSizeCalculator.getObjectSize(kvs));
        System.out.println("Stage1 : " + (tag2 - tag1));
        System.out.println("Stage3 : " + (tag6 - tag5));

        int[] index = new int[]{4,5,10};
        long tag3 = System.currentTimeMillis();
        for (KeyValue kv : kvs) {
            rd.decodeValue(kv, index);
        }
        long tag4 = System.currentTimeMillis();

        System.out.println("Stage2 : " + (tag4 - tag3));

    }

    public static void test(String[] args) {
        List<DingoSchema> table = getTable();
        RecordEncoder re = new RecordEncoder(0, table, 0L);
        RecordDecoder rd = new RecordDecoder(0, table, 0L);

        List<Object[]> records = getRecords();

        long tag1 = System.currentTimeMillis();
        for(Object[] record : records) {
            KeyValue kv = re.encode(record);
        }
        long tag2 = System.currentTimeMillis();

        List<KeyValue> kvs = new ArrayList<>();
        for(Object[] record : records) {
            KeyValue kv = re.encode(record);
            kvs.add(kv);
        }

        int[] index = new int[]{4,5,10};
        long tag3 = System.currentTimeMillis();
        for (KeyValue kv : kvs) {
            Object[] record = rd.decodeValue(kv, index);
            record[4] = record[4] + "a";
            record[5] = false;
            record[10] = (double)record[10] + 1;
            kv.setValue(re.updateValueByRecord(kv.getValue(), record, index));
        }
        long tag4 = System.currentTimeMillis();

        long tag5 = System.currentTimeMillis();
        for(KeyValue kv : kvs) {
            Object[] record = rd.decode(kv);
        }
        long tag6 = System.currentTimeMillis();

        System.out.println(ObjectSizeCalculator.getObjectSize(kvs));
        System.out.println("Stage1 : " + (tag2 - tag1));
        System.out.println("Stage2 : " + (tag4 - tag3));
        System.out.println("Stage3 : " + (tag6 - tag5));
    }

    public static void testDecodeKeyPrefix() {
        List<DingoSchema> table = getTable();
        RecordEncoder re = new RecordEncoder(0, table, 0L);
        RecordDecoder rd = new RecordDecoder(0, table, 0L);

        Object[] record = new Object[14];
        record[0] = 1;
        record[1] = "test address test 中文 表情\uD83D\uDE0A\uD83C\uDFF7️\uD83D\uDC4C test 测试测试测试三" +
                "\uD83E\uDD23\uD83D\uDE02\uD83D\uDE01\uD83D\uDC31\u200D\uD83D\uDC09\uD83D\uDC4F\uD83D\uDC31" +
                "\u200D\uD83D\uDCBB✔\uD83E\uDD33\uD83E\uDD26\u200D♂️\uD83E\uDD26\u200D♀️\uD83D\uDE4C" +
                "测试测试测试伍佰肆拾陆万伍仟陆佰伍拾肆元/n/r/r/ndfs肥肉士大夫";
        record[2] = "f";


        byte[] keyPrefix = re.encodeKeyPrefix(record, 3);
        Object[] keyFromPrefix = rd.decodeKeyPrefix(keyPrefix);
        System.out.println(keyFromPrefix);

    }

    private static List<DingoSchema> getTable() {
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

        DingoSchema testFloat = new FloatSchema();
        testFloat.setIndex(11);
        testFloat.setAllowNull(true);
        testFloat.setIsKey(false);

        DingoSchema testBooleanList1 = new BooleanListSchema();
        testBooleanList1.setIndex(12);
        testBooleanList1.setAllowNull(true);
        testBooleanList1.setIsKey(false);

        DingoSchema testBooleanList2 = new BooleanListSchema();
        testBooleanList2.setIndex(13);
        testBooleanList2.setAllowNull(false);
        testBooleanList2.setIsKey(false);

        DingoSchema testStringList1 = new StringListSchema();
        testStringList1.setIndex(14);
        testStringList1.setAllowNull(true);
        testStringList1.setIsKey(false);

        DingoSchema testStringList2 = new StringListSchema();
        testStringList2.setIndex(15);
        testStringList2.setAllowNull(false);
        testStringList2.setIsKey(false);

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
        table.add(testFloat);
        table.add(testBooleanList1);
        table.add(testBooleanList2);
        table.add(testStringList1);
        table.add(testStringList2);

        return table;
    }

    private static List<Object[]> getRecords() {
        byte[] k1 = new byte[1024];
        for (int i = 0; i < 1024; i++) {
            k1[i] = ((byte) (i%256));
        }

        List<Boolean> b1 = new ArrayList<>();
        b1.add(false);
        b1.add(true);

        List<Boolean> b2 = new ArrayList<>();
        b2.add(false);
        b2.add(true);
        b2.add(false);
        b2.add(false);

        List<String> s1 = new ArrayList<>();

        List<String> s2 = new ArrayList<>();
        s2.add("qwe");
        s2.add("中文");

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
                873485.4234,
                873485.42F,
                b1,
                b2,
                s1,
                s2
        };

        List<Object[]> allRecord = new ArrayList<>();
        for (int i = 0; i < 1000000; i ++) {
            Object[] r = new Object[16];
            System.arraycopy(record, 1, r, 1, 15);
            r[0] = i;
            allRecord.add(r);
        }

        return allRecord;
    }
}