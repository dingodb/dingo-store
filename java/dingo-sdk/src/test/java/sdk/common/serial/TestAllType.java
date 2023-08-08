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
import lombok.val;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class TestAllType {

    private RecordDecoder recordDecoder;
    private RecordEncoder recordEncoder;

    private List<DingoSchema> schemas;
    @Before
    public void setUp() {
        // 初始化RecordDecoder
        int schemaVersion = 1;
        schemas = getTable();
        long commonId = 123456789L;
        recordEncoder = new RecordEncoder(schemaVersion, schemas, commonId);
        recordDecoder = new RecordDecoder(schemaVersion, schemas, commonId);
    }
    @Test
    public void testEncodeAndDecode() {

        Object[] records = getRecords();

        KeyValue kv = recordEncoder.encode(records);

        Object[] result = recordDecoder.decode(kv);
        // Positive test case
        Assert.assertNotNull(result);
        Assert.assertEquals(records.length, result.length);
        // Negative test case
        for (int i = 0; i < schemas.size(); i++) {
            System.out.println(records[i]+","+result[i]);
            switch (schemas.get(i).getType()) {
                case BYTES:
                    byte[] e = (byte[]) records[i];
                    byte[] r = (byte[]) result[i];
                    Assert.assertArrayEquals(e, r);
                    break;
                case BOOLEANLIST:
                case STRINGLIST:
                case DOUBLELIST:
                case FLOATLIST:
                case INTEGERLIST:
                case LONGLIST:
                    val e_val = records[i];
                    val r_cal = result[i];
                    Assert.assertTrue(e_val.equals(r_cal));
                    break;
                default:
                    Assert.assertEquals(records[i], result[i]);
                    break;
            }
        }
    }
    @Test
    public void testDecodeWithColumnIndexes() {

        Object[] records = getRecords();

        KeyValue kv = recordEncoder.encode(records);

        int[] columnIndexes = new int[]{0, 3, 6, 8, 10, 11, 13, 15, 16, 18, 19, 21, 22};
        Object[] result = recordDecoder.decode(kv, columnIndexes);
        // Positive test case
        Assert.assertNotNull(result);
        Assert.assertEquals(columnIndexes.length, result.length);
        // Negative test case
        int j = 0;
        for (int i = 0; i < schemas.size(); i++) {
            if(j < columnIndexes.length)
                System.out.println(records[i]+","+result[j]);
            switch (schemas.get(i).getType()) {
                case BYTES:
                    if (isValueInArray(i, columnIndexes)) {
                        byte[] e = (byte[]) records[i];
                        byte[] r = (byte[]) result[j];
                        Assert.assertArrayEquals(e, r);
                        j++;
                    }
                    break;
                case BOOLEANLIST:
                case STRINGLIST:
                case DOUBLELIST:
                case FLOATLIST:
                case INTEGERLIST:
                case LONGLIST:
                    if (isValueInArray(i, columnIndexes)) {
                        val e_val = records[i];
                        val r_cal = result[j];
                        Assert.assertTrue(e_val.equals(r_cal));
                        j++;
                    }
                    break;
                default:
                    if (isValueInArray(i, columnIndexes)) {
                        Assert.assertEquals(records[i], result[j]);
                        j++;
                    }
                    break;
            }
        }
    }

    private static boolean isValueInArray(int i, int[] columnIndexes) {
        int index = Arrays.binarySearch(columnIndexes, i);
        return index >= 0;
    }
    private static List<DingoSchema> getTable() {
        DingoSchema id = new IntegerSchema();
        id.setIndex(0);
        id.setAllowNull(false);
        id.setIsKey(true);

        DingoSchema<String> name = new StringSchema();
        name.setIndex(1);
        name.setAllowNull(false);
        name.setIsKey(true);

        DingoSchema<String> gender = new StringSchema();
        gender.setIndex(2);
        gender.setAllowNull(false);
        gender.setIsKey(true);

        DingoSchema<Long> score = new LongSchema();
        score.setIndex(3);
        score.setAllowNull(false);
        score.setIsKey(true);

        DingoSchema addr = new StringSchema();
        addr.setIndex(4);
        addr.setAllowNull(true);
        addr.setIsKey(false);

        DingoSchema<Boolean> exist = new BooleanSchema();
        exist.setIndex(5);
        exist.setAllowNull(false);
        exist.setIsKey(false);

        DingoSchema<byte[]> pic = new BytesSchema();
        pic.setIndex(6);
        pic.setAllowNull(true);
        pic.setIsKey(false);

        DingoSchema<Integer> testNull = new IntegerSchema();
        testNull.setIndex(7);
        testNull.setAllowNull(true);
        testNull.setIsKey(false);

        DingoSchema<Integer> age = new IntegerSchema();
        age.setIndex(8);
        age.setAllowNull(false);
        age.setIsKey(false);

        DingoSchema<Long> prev = new LongSchema();
        prev.setIndex(9);
        prev.setAllowNull(false);
        prev.setIsKey(false);

        DingoSchema<Double> salary = new DoubleSchema();
        salary.setIndex(10);
        salary.setAllowNull(true);
        salary.setIsKey(false);

        DingoSchema<Float> testFloat = new FloatSchema();
        testFloat.setIndex(11);
        testFloat.setAllowNull(true);
        testFloat.setIsKey(false);

        DingoSchema<List<Boolean>> testBooleanList1 = new BooleanListSchema();
        testBooleanList1.setIndex(12);
        testBooleanList1.setAllowNull(true);
        testBooleanList1.setIsKey(false);

        DingoSchema<List<Boolean>> testBooleanList2 = new BooleanListSchema();
        testBooleanList2.setIndex(13);
        testBooleanList2.setAllowNull(false);
        testBooleanList2.setIsKey(false);

        DingoSchema<List<String>> testStringList1 = new StringListSchema();
        testStringList1.setIndex(14);
        testStringList1.setAllowNull(true);
        testStringList1.setIsKey(false);

        DingoSchema testStringList2 = new StringListSchema();
        testStringList2.setIndex(15);
        testStringList2.setAllowNull(false);
        testStringList2.setIsKey(false);

        DingoSchema<List<Double>> testDoubleList1 = new DoubleListSchema();
        testDoubleList1.setIndex(16);
        testDoubleList1.setAllowNull(true);
        testDoubleList1.setIsKey(false);

        DingoSchema<List<Double>> testDoubleList2 = new DoubleListSchema();
        testDoubleList2.setIndex(17);
        testDoubleList2.setAllowNull(false);
        testDoubleList2.setIsKey(false);

        DingoSchema<List<Float>> testFloatList1 = new FloatListSchema();
        testFloatList1.setIndex(18);
        testFloatList1.setAllowNull(true);
        testFloatList1.setIsKey(false);

        DingoSchema<List<Float>> testFloatList2 = new FloatListSchema();
        testFloatList2.setIndex(19);
        testFloatList2.setAllowNull(false);
        testFloatList2.setIsKey(false);

        DingoSchema<List<Integer>> testIntegerList1 = new IntegerListSchema();
        testIntegerList1.setIndex(20);
        testIntegerList1.setAllowNull(true);
        testIntegerList1.setIsKey(false);

        DingoSchema<List<Integer>> testIntegerList2 = new IntegerListSchema();
        testIntegerList2.setIndex(21);
        testIntegerList2.setAllowNull(false);
        testIntegerList2.setIsKey(false);

        DingoSchema<List<Long>> testLongList1 = new LongListSchema();
        testLongList1.setIndex(22);
        testLongList1.setAllowNull(true);
        testLongList1.setIsKey(false);

        DingoSchema<List<Long>> testLongList2 = new LongListSchema();
        testLongList2.setIndex(23);
        testLongList2.setAllowNull(true);
        testLongList2.setIsKey(false);

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
        table.add(testDoubleList1);
        table.add(testDoubleList2);
        table.add(testFloatList1);
        table.add(testFloatList2);
        table.add(testIntegerList1);
        table.add(testIntegerList2);
        table.add(testLongList1);
        table.add(testLongList2);

        return table;
    }

    private static Object[] getRecords() {
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

        List<Double> d1 = new ArrayList<>();

        List<Double> d2 = new ArrayList<>();
        d2.add(3323232333.21221);
        d2.add(23232.2111111);
        d2.add(222334455566.23);
        d2.add(2222d);
        d2.add(67889.246);

        List<Float> f1 = new ArrayList<>();

        List<Float> f2 = new ArrayList<>();
        f2.add(33232.21221f);
        f2.add(23232.2111111f);
        f2.add(2222f);
        f2.add(67889.246f);

        List<Integer> i1 = new ArrayList<>();

        List<Integer> i2 = new ArrayList<>();
        i2.add(33232);
        i2.add(2111111);
        i2.add(2222);
        i2.add(246);

        List<Long> l1 = new ArrayList<>();

        List<Long> l2 = new ArrayList<>();
        l2.add(33232998776555l);
        l2.add(2111111l);
        l2.add(2222l);
        l2.add(2469999883732l);

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
                s2,
                d1,
                d2,
                f1,
                f2,
                i1,
                i2,
                l1,
                l2
        };
        return record;
    }
}