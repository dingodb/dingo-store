package io.dingodb.serial;

import io.dingodb.serial.v1.io.RecordDecoder;
import io.dingodb.serial.v1.io.RecordEncoder;
import io.dingodb.serial.v1.schema.*;
import io.dingodb.sdk.common.KeyValue;
import org.openjdk.jmh.annotations.*;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
public class FinalTest {

    private List<Object[]> records = getRecords();

    private List<List<DingoSchema>> V1Table = getV1Table();
    private List<io.dingodb.serial.v2.t1.schema.DingoSchema> V21Table = getV21Table();
    private List<io.dingodb.serial.v2.t2.schema.DingoSchema> V22Table = getV22Table();

    private RecordEncoder reKeyV1 =
            new RecordEncoder(V1Table.get(0), (short) 0, (byte) 0, (byte) 0, (byte) 0, null);
    private RecordEncoder reValueV1 =
            new RecordEncoder(V1Table.get(1), (short) 0, (byte) 0, (byte) 0, (byte) 0, null);
    private RecordDecoder rdKeyV1 =
            new RecordDecoder(V1Table.get(0), (short) 0, (byte) 0, (byte) 0, (byte) 0, null, true);
    private RecordDecoder rdValueV1 =
            new RecordDecoder(V1Table.get(1), (short) 0, (byte) 0, (byte) 0, (byte) 0, null, false);

    private io.dingodb.serial.v2.t1.RecordEncoder reV21 = new io.dingodb.serial.v2.t1.RecordEncoder(0, V21Table);
    private io.dingodb.serial.v2.t1.RecordDecoder rdV21 = new io.dingodb.serial.v2.t1.RecordDecoder(0, V21Table);

    private io.dingodb.serial.v2.t2.RecordEncoder reV22 = new io.dingodb.serial.v2.t2.RecordEncoder(0, V22Table);
    private io.dingodb.serial.v2.t2.RecordDecoder rdV22 = new io.dingodb.serial.v2.t2.RecordDecoder(0, V22Table);

    private List<KeyValue> v1Kv = getV1Kv();
    private List<KeyValue> v21Kv = getV21Kv();
    private List<List<KeyValue>> v22Kvs = getV22Kvs();
    private List<List<KeyValue>> v22Kvss = getV22Kvss();

    private int[] index = new int[]{4,5,10};

    private Object[] result = new Object[] {"test address test 中文 表情\uD83D\uDE0A\uD83C\uDFF7️\uD83D\uDC4C test 测试测试测试三" +
            "\uD83E\uDD23\uD83D\uDE02\uD83D\uDE01\uD83D\uDC31\u200D\uD83D\uDC09\uD83D\uDC4F\uD83D\uDC31" +
            "\u200D\uD83D\uDCBB✔\uD83E\uDD33\uD83E\uDD26\u200D♂️\uD83E\uDD26\u200D♀️\uD83D\uDE4C" +
            "测试测试测试伍佰肆拾陆万伍仟陆佰伍拾肆元/n/r/r/ndfs肥肉士大夫a", false, 873486.4234};

    private Object[] record = new Object[] {
            null,
            null,
            null,
            null,
            "test address test 中文 表情\uD83D\uDE0A\uD83C\uDFF7️\uD83D\uDC4C test 测试测试测试三" +
                    "\uD83E\uDD23\uD83D\uDE02\uD83D\uDE01\uD83D\uDC31\u200D\uD83D\uDC09\uD83D\uDC4F\uD83D\uDC31" +
                    "\u200D\uD83D\uDCBB✔\uD83E\uDD33\uD83E\uDD26\u200D♂️\uD83E\uDD26\u200D♀️\uD83D\uDE4C" +
                    "测试测试测试伍佰肆拾陆万伍仟陆佰伍拾肆元/n/r/r/ndfs肥肉士大夫a",
            false,
            null,
            null,
            null,
            null,
            873486.4234
    };

    @Benchmark()
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    @BenchmarkMode(Mode.AverageTime)
    public void insertAllV1() throws Exception {
        for (Object[] record : records) {
            byte[] key = reKeyV1.encodeKey(record);
            byte[] value = reValueV1.encode(record);
            KeyValue kv = new KeyValue(null, null);
            kv.setKey(key);
            kv.setValue(value);
        }
    }

    @Benchmark()
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    @BenchmarkMode(Mode.AverageTime)
    public void insertAllV21() {
        for(Object[] record : records) {
            KeyValue kv = reV21.encode(record);
        }
    }

    @Benchmark()
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    @BenchmarkMode(Mode.AverageTime)
    public void insertAllV22() {
        for(Object[] record : records) {
            List<KeyValue> kvs = reV22.encode(record);
        }
    }

    @Benchmark()
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    @BenchmarkMode(Mode.AverageTime)
    public void selectAllV1() throws Exception {
        for (KeyValue kv : v1Kv) {
            Object[] record1 = rdValueV1.decode(kv.getValue());
        }
    }

    @Benchmark()
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    @BenchmarkMode(Mode.AverageTime)
    public void selectAllV21() throws Exception {
        for (KeyValue kv : v21Kv) {
            Object[] record = rdV21.decode(kv);
        }
    }

    @Benchmark()
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    @BenchmarkMode(Mode.AverageTime)
    public void selectAllV22() throws Exception {
        for(List<KeyValue> kvs : v22Kvs) {
            Object[] record = rdV22.decode(kvs);
        }
    }

    @Benchmark()
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    @BenchmarkMode(Mode.AverageTime)
    public void select45xV1() throws Exception {
        for (KeyValue kv : v1Kv) {
            Object[] record = rdValueV1.decode(kv.getValue(), index);
        }
    }

    @Benchmark()
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    @BenchmarkMode(Mode.AverageTime)
    public void select45xV21() throws Exception {
        for (KeyValue kv : v21Kv) {
            Object[] record = rdV21.decodeValue(kv, index);
        }
    }

    @Benchmark()
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    @BenchmarkMode(Mode.AverageTime)
    public void select45xV22() throws Exception {
        for(List<KeyValue> kvs : v22Kvss) {
            Object[] record = rdV22.decode(kvs, index);
        }
    }

    @Benchmark()
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    @BenchmarkMode(Mode.AverageTime)
    public void update45xV1() throws Exception {
        for (KeyValue kv : v1Kv) {
            kv.setValue(reValueV1.encode(kv.getValue(), index, result));
        }
    }

    @Benchmark()
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    @BenchmarkMode(Mode.AverageTime)
    public void update45xV21() {
        for (KeyValue kv : v21Kv) {
            reV21.updateValueByRecord(kv.getValue(), record, index);
        }
    }

    @Benchmark()
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    @BenchmarkMode(Mode.AverageTime)
    public void update45xV22() {
        for (List<KeyValue> ukvs : v22Kvss) {
            reV22.updateValueByRecord(ukvs, record, index);
        }
    }






    private List<KeyValue> getV1Kv() {
        try {
            List<KeyValue> kvs = new ArrayList<>();
            for (Object[] record : records) {
                byte[] key = reKeyV1.encodeKey(record);
                byte[] value = reValueV1.encode(record);
                KeyValue kv = new KeyValue(null, null);
                kv.setKey(key);
                kv.setValue(value);
                kvs.add(kv);
            }
            return kvs;
        } catch (Exception e) {
            return null;
        }
    }

    private List<KeyValue> getV21Kv() {
        List<KeyValue> kvs = new ArrayList<>();
        for(Object[] record : records) {
            KeyValue kv = reV21.encode(record);
            kvs.add(kv);
        }
        return kvs;
    }

    private List<List<KeyValue>> getV22Kvs() {
        List<List<KeyValue>> kvss = new ArrayList<>();
        for(Object[] record : records) {
            List<KeyValue> kvs = reV22.encode(record);
            kvss.add(kvs);
        }
        return kvss;
    }

    private List<List<KeyValue>> getV22Kvss() {
        List<List<KeyValue>> kvss = new ArrayList<>();
        for (List<KeyValue> kvs : v22Kvs) {
            List<KeyValue> ukvs = new ArrayList<>(2);
            ukvs.add(kvs.get(0));
            ukvs.add(kvs.get(1));
            kvss.add(ukvs);
        }
        return kvss;
    }

    private List<io.dingodb.serial.v2.t2.schema.DingoSchema> getV22Table() {
        io.dingodb.serial.v2.t2.schema.DingoSchema id = new io.dingodb.serial.v2.t2.schema.IntegerSchema();
        id.setIndex(0);
        id.setAllowNull(false);
        id.setIsKey(true);

        io.dingodb.serial.v2.t2.schema.DingoSchema name = new io.dingodb.serial.v2.t2.schema.StringSchema();
        name.setIndex(1);
        name.setAllowNull(false);
        name.setIsKey(true);

        io.dingodb.serial.v2.t2.schema.DingoSchema gender = new io.dingodb.serial.v2.t2.schema.StringSchema();
        gender.setIndex(2);
        gender.setAllowNull(false);
        gender.setIsKey(true);

        io.dingodb.serial.v2.t2.schema.DingoSchema score = new io.dingodb.serial.v2.t2.schema.LongSchema();
        score.setIndex(3);
        score.setAllowNull(false);
        score.setIsKey(true);



        io.dingodb.serial.v2.t2.schema.DingoSchema addr = new io.dingodb.serial.v2.t2.schema.StringSchema();
        addr.setIndex(4);
        addr.setAllowNull(true);
        addr.setIsKey(false);

        io.dingodb.serial.v2.t2.schema.DingoSchema exist = new io.dingodb.serial.v2.t2.schema.BooleanSchema();
        exist.setIndex(5);
        exist.setAllowNull(false);
        exist.setIsKey(false);

        io.dingodb.serial.v2.t2.schema.DingoSchema pic = new io.dingodb.serial.v2.t2.schema.BytesSchema();
        pic.setIndex(6);
        pic.setAllowNull(true);
        pic.setIsKey(false);

        io.dingodb.serial.v2.t2.schema.DingoSchema testNull = new io.dingodb.serial.v2.t2.schema.IntegerSchema();
        testNull.setIndex(7);
        testNull.setAllowNull(true);
        testNull.setIsKey(false);

        io.dingodb.serial.v2.t2.schema.DingoSchema age = new io.dingodb.serial.v2.t2.schema.IntegerSchema();
        age.setIndex(8);
        age.setAllowNull(false);
        age.setIsKey(false);

        io.dingodb.serial.v2.t2.schema.DingoSchema prev = new io.dingodb.serial.v2.t2.schema.LongSchema();
        prev.setIndex(9);
        prev.setAllowNull(false);
        prev.setIsKey(false);

        io.dingodb.serial.v2.t2.schema.DingoSchema salary = new io.dingodb.serial.v2.t2.schema.DoubleSchema();
        salary.setIndex(10);
        salary.setAllowNull(true);
        salary.setIsKey(false);


        List<io.dingodb.serial.v2.t2.schema.DingoSchema> table = new ArrayList<>();
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

        return table;
    }



    private List<io.dingodb.serial.v2.t1.schema.DingoSchema> getV21Table() {
        io.dingodb.serial.v2.t1.schema.DingoSchema id = new io.dingodb.serial.v2.t1.schema.IntegerSchema();
        id.setIndex(0);
        id.setAllowNull(false);
        id.setIsKey(true);

        io.dingodb.serial.v2.t1.schema.DingoSchema name = new io.dingodb.serial.v2.t1.schema.StringSchema();
        name.setIndex(1);
        name.setAllowNull(false);
        name.setIsKey(true);

        io.dingodb.serial.v2.t1.schema.DingoSchema gender = new io.dingodb.serial.v2.t1.schema.StringSchema();
        gender.setIndex(2);
        gender.setAllowNull(false);
        gender.setIsKey(true);

        io.dingodb.serial.v2.t1.schema.DingoSchema score = new io.dingodb.serial.v2.t1.schema.LongSchema();
        score.setIndex(3);
        score.setAllowNull(false);
        score.setIsKey(true);



        io.dingodb.serial.v2.t1.schema.DingoSchema addr = new io.dingodb.serial.v2.t1.schema.StringSchema();
        addr.setIndex(4);
        addr.setAllowNull(true);
        addr.setIsKey(false);

        io.dingodb.serial.v2.t1.schema.DingoSchema exist = new io.dingodb.serial.v2.t1.schema.BooleanSchema();
        exist.setIndex(5);
        exist.setAllowNull(false);
        exist.setIsKey(false);

        io.dingodb.serial.v2.t1.schema.DingoSchema pic = new io.dingodb.serial.v2.t1.schema.BytesSchema();
        pic.setIndex(6);
        pic.setAllowNull(true);
        pic.setIsKey(false);

        io.dingodb.serial.v2.t1.schema.DingoSchema testNull = new io.dingodb.serial.v2.t1.schema.IntegerSchema();
        testNull.setIndex(7);
        testNull.setAllowNull(true);
        testNull.setIsKey(false);

        io.dingodb.serial.v2.t1.schema.DingoSchema age = new io.dingodb.serial.v2.t1.schema.IntegerSchema();
        age.setIndex(8);
        age.setAllowNull(false);
        age.setIsKey(false);

        io.dingodb.serial.v2.t1.schema.DingoSchema prev = new io.dingodb.serial.v2.t1.schema.LongSchema();
        prev.setIndex(9);
        prev.setAllowNull(false);
        prev.setIsKey(false);

        io.dingodb.serial.v2.t1.schema.DingoSchema salary = new io.dingodb.serial.v2.t1.schema.DoubleSchema();
        salary.setIndex(10);
        salary.setAllowNull(true);
        salary.setIsKey(false);


        List<io.dingodb.serial.v2.t1.schema.DingoSchema> table = new ArrayList<>();
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

        return table;
    }


    private List<List<DingoSchema>> getV1Table() {
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

    private List<Object[]> getRecords() {
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
