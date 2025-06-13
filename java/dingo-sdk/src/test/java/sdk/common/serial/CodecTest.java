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
import io.dingodb.sdk.common.codec.DingoKeyValueCodec;
import io.dingodb.sdk.common.table.ColumnDefinition;
import io.dingodb.sdk.common.table.TableDefinition;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Date;

public class CodecTest {

    @Test
    public void test() throws IOException {

        ColumnDefinition c1 = ColumnDefinition.builder().name("col1").type("VARCHAR").primary(0).build();
        ColumnDefinition c2 = ColumnDefinition.builder().name("col2").type("BOOL").primary(1).build();
        ColumnDefinition c3 = ColumnDefinition.builder().name("col3").type("DOUBLE").scale(1).primary(-1).build();
        ColumnDefinition c4 = ColumnDefinition.builder().name("col4").type("LONG").primary(-1).build();

        TableDefinition tableDefinition = TableDefinition.builder()
            .name("TEST_ENCODE")
            .columns(Arrays.asList(c1, c2, c3, c4))
            .version(1).codecVersion(2)
            .build();

        DingoKeyValueCodec codec = DingoKeyValueCodec.of(1, 1L, tableDefinition);
        Object[] record = {"id1_1", true, 1.02, 123L};
        Assertions.assertArrayEquals(record, codec.decode(codec.encode(record)));

    }

    @Test
    public void testDecimal1() throws IOException {

        ColumnDefinition c1 = ColumnDefinition.builder().name("col1").type("DECIMAL").primary(0).precision(10).scale(4).build();
        ColumnDefinition c2 = ColumnDefinition.builder().name("col2").type("INT").primary(-1).build();

        TableDefinition tableDefinition = TableDefinition.builder()
                .name("TEST_ENCODE")
                .columns(Arrays.asList(c1, c2))
                .version(1).codecVersion(2)
                .build();

        DingoKeyValueCodec codec = DingoKeyValueCodec.of(1, 1L, tableDefinition);
        Object[] record = {"123.123", 1};
        Object[] expected = {"123.1230", 1};
        Assertions.assertArrayEquals(expected, codec.decode(codec.encode(record)));
    }

    @Test
    public void testDecimal1_1() throws IOException {

        ColumnDefinition c1 = ColumnDefinition.builder().name("col1").type("DECIMAL").primary(0).precision(10).scale(2).build();
        ColumnDefinition c2 = ColumnDefinition.builder().name("col2").type("INT").primary(-1).build();

        TableDefinition tableDefinition = TableDefinition.builder()
                .name("TEST_ENCODE")
                .columns(Arrays.asList(c1, c2))
                .version(1).codecVersion(2)
                .build();

        DingoKeyValueCodec codec = DingoKeyValueCodec.of(1, 1L, tableDefinition);
        Object[] record = {"1.231234e2", 1};
        Object[] expected = {"123.12", 1};
        Assertions.assertArrayEquals(expected, codec.decode(codec.encode(record)));
    }

    @Test
    public void testDecimal1_2() throws IOException {

        ColumnDefinition c1 = ColumnDefinition.builder().name("col1").type("DECIMAL").primary(0).precision(10).scale(2).build();
        ColumnDefinition c2 = ColumnDefinition.builder().name("col2").type("INT").primary(-1).build();

        TableDefinition tableDefinition = TableDefinition.builder()
                .name("TEST_ENCODE")
                .columns(Arrays.asList(c1, c2))
                .version(1).codecVersion(2)
                .build();

        DingoKeyValueCodec codec = DingoKeyValueCodec.of(1, 1L, tableDefinition);
        Object[] record = {"2345.678", 1};
        Object[] expected = {"2345.68", 1};
        Assertions.assertArrayEquals(expected, codec.decode(codec.encode(record)));
    }

    @Test
    public void testDecimal1_3() throws IOException {

        ColumnDefinition c1 = ColumnDefinition.builder().name("col1").type("DECIMAL").primary(0).precision(10).scale(2).build();
        ColumnDefinition c2 = ColumnDefinition.builder().name("col2").type("INT").primary(-1).build();

        TableDefinition tableDefinition = TableDefinition.builder()
                .name("TEST_ENCODE")
                .columns(Arrays.asList(c1, c2))
                .version(1).codecVersion(2)
                .build();

        DingoKeyValueCodec codec = DingoKeyValueCodec.of(1, 1L, tableDefinition);
        Object[] record = {"2345.6", 1};
        Object[] expected = {"2345.60", 1};
        Assertions.assertArrayEquals(expected, codec.decode(codec.encode(record)));
    }

    @Test
    public void testDecimal1_4() throws IOException {

        ColumnDefinition c1 = ColumnDefinition.builder().name("col1").type("DECIMAL").primary(0).precision(10).scale(2).build();
        ColumnDefinition c2 = ColumnDefinition.builder().name("col2").type("INT").primary(-1).build();

        TableDefinition tableDefinition = TableDefinition.builder()
                .name("TEST_ENCODE")
                .columns(Arrays.asList(c1, c2))
                .version(1).codecVersion(2)
                .build();

        DingoKeyValueCodec codec = DingoKeyValueCodec.of(1, 1L, tableDefinition);
        Object[] record = {"2345", 1};
        Object[] expected = {"2345.00", 1};
        Assertions.assertArrayEquals(expected, codec.decode(codec.encode(record)));
    }

    @Test
    public void testDecimal1_5() throws IOException {

        ColumnDefinition c1 = ColumnDefinition.builder().name("col1").type("DECIMAL").primary(0).precision(10).scale(2).build();
        ColumnDefinition c2 = ColumnDefinition.builder().name("col2").type("INT").primary(-1).build();

        TableDefinition tableDefinition = TableDefinition.builder()
                .name("TEST_ENCODE")
                .columns(Arrays.asList(c1, c2))
                .version(1).codecVersion(2)
                .build();

        DingoKeyValueCodec codec = DingoKeyValueCodec.of(1, 1L, tableDefinition);
        Object[] record = {"-2345.678", 1};
        Object[] expected = {"-2345.68", 1};
        Assertions.assertArrayEquals(expected, codec.decode(codec.encode(record)));
    }

    @Test
    public void testDecimal2() throws IOException {

        ColumnDefinition c1 = ColumnDefinition.builder().name("col1").type("DECIMAL").primary(0).precision(10).scale(3).build();
        ColumnDefinition c2 = ColumnDefinition.builder().name("col2").type("INT").primary(-1).build();

        TableDefinition tableDefinition = TableDefinition.builder()
                .name("TEST_ENCODE")
                .columns(Arrays.asList(c1, c2))
                .version(1).codecVersion(2)
                .build();

        DingoKeyValueCodec codec = DingoKeyValueCodec.of(1, 1L, tableDefinition);
        Object[] record = {"-123.45", 1};
        Object[] expected = {"-123.450", 1};
        Assertions.assertArrayEquals(expected, codec.decode(codec.encode(record)));
    }

    @Test
    public void testDecimal3() throws IOException {

        ColumnDefinition c1 = ColumnDefinition.builder().name("col1").type("DECIMAL").primary(0).precision(40).scale(2).build();
        ColumnDefinition c2 = ColumnDefinition.builder().name("col2").type("INT").primary(-1).build();

        TableDefinition tableDefinition = TableDefinition.builder()
                .name("TEST_ENCODE")
                .columns(Arrays.asList(c1, c2))
                .version(1).codecVersion(2)
                .build();

        DingoKeyValueCodec codec = DingoKeyValueCodec.of(1, 1L, tableDefinition);
        Object[] record = {"1.00", 1};
        Object[] expected = {"1.00", 1};
        Assertions.assertArrayEquals(expected, codec.decode(codec.encode(record)));

        Object[] record1 = {"111111111111111111111111111111111111.11", 1};
        Object[] expected1 = {"111111111111111111111111111111111111.11", 1};
        Assertions.assertArrayEquals(expected1, codec.decode(codec.encode(record1)));
    }

    @Test
    public void testDecimal4() throws IOException {

        ColumnDefinition c1 = ColumnDefinition.builder().name("col1").type("DECIMAL").primary(0).precision(4).scale(2).build();
        ColumnDefinition c2 = ColumnDefinition.builder().name("col2").type("INT").primary(-1).build();

        TableDefinition tableDefinition = TableDefinition.builder()
                .name("TEST_ENCODE")
                .columns(Arrays.asList(c1, c2))
                .version(1).codecVersion(2)
                .build();

        DingoKeyValueCodec codec = DingoKeyValueCodec.of(1, 1L, tableDefinition);
        Object[] record = {"-10.55", 1};
        Object[] expected = {"-10.55", 1};
        Assertions.assertArrayEquals(expected, codec.decode(codec.encode(record)));
    }

    @Test
    public void testDecimal5() throws IOException {

        ColumnDefinition c1 = ColumnDefinition.builder().name("col1").type("DECIMAL").primary(0).precision(30).scale(25).build();
        ColumnDefinition c2 = ColumnDefinition.builder().name("col2").type("INT").primary(-1).build();

        TableDefinition tableDefinition = TableDefinition.builder()
                .name("TEST_ENCODE")
                .columns(Arrays.asList(c1, c2))
                .version(1).codecVersion(2)
                .build();

        DingoKeyValueCodec codec = DingoKeyValueCodec.of(1, 1L, tableDefinition);
        Object[] record = {"0.0123456789012345678912345", 1};
        Object[] expected = {"0.0123456789012345678912345", 1};
        Assertions.assertArrayEquals(expected, codec.decode(codec.encode(record)));
    }

    @Test
    public void testDecimal6() throws IOException {

        ColumnDefinition c1 = ColumnDefinition.builder().name("col1").type("DECIMAL").primary(0).precision(5).scale(0).build();
        ColumnDefinition c2 = ColumnDefinition.builder().name("col2").type("INT").primary(-1).build();

        TableDefinition tableDefinition = TableDefinition.builder()
                .name("TEST_ENCODE")
                .columns(Arrays.asList(c1, c2))
                .version(1).codecVersion(2)
                .build();

        DingoKeyValueCodec codec = DingoKeyValueCodec.of(1, 1L, tableDefinition);
        Object[] record = {"12345", 1};
        Object[] expected = {"12345", 1};
        Assertions.assertArrayEquals(expected, codec.decode(codec.encode(record)));
    }

    @Test
    public void testDecimal7() throws IOException {

        ColumnDefinition c1 = ColumnDefinition.builder().name("col1").type("DECIMAL").primary(0).precision(10).scale(3).build();
        ColumnDefinition c2 = ColumnDefinition.builder().name("col2").type("INT").primary(-1).build();

        TableDefinition tableDefinition = TableDefinition.builder()
                .name("TEST_ENCODE")
                .columns(Arrays.asList(c1, c2))
                .version(1).codecVersion(2)
                .build();

        DingoKeyValueCodec codec = DingoKeyValueCodec.of(1, 1L, tableDefinition);
        Object[] record = {"12345", 1};
        Object[] expected = {"12345.000", 1};
        Assertions.assertArrayEquals(expected, codec.decode(codec.encode(record)));

        Object[] record1 = {"123.45", 1};
        Object[] expected1 = {"123.45", 1};
        Assertions.assertArrayEquals(expected, codec.decode(codec.encode(record)));
    }

    @Test
    public void testDecimal8() throws IOException {

        ColumnDefinition c1 = ColumnDefinition.builder().name("col1").type("DECIMAL").primary(0).precision(20).scale(10).build();
        ColumnDefinition c2 = ColumnDefinition.builder().name("col2").type("INT").primary(-1).build();

        TableDefinition tableDefinition = TableDefinition.builder()
                .name("TEST_ENCODE")
                .columns(Arrays.asList(c1, c2))
                .version(1).codecVersion(2)
                .build();

        DingoKeyValueCodec codec = DingoKeyValueCodec.of(1, 1L, tableDefinition);
        Object[] record = {"-123.45", 1};
        Object[] expected = {"-123.4500000000", 1};
        Assertions.assertArrayEquals(expected, codec.decode(codec.encode(record)));
    }

    @Test
    public void testDecimal9() throws IOException {

        ColumnDefinition c1 = ColumnDefinition.builder().name("col1").type("DECIMAL").primary(0).precision(22).scale(20).build();
        ColumnDefinition c2 = ColumnDefinition.builder().name("col2").type("INT").primary(-1).build();

        TableDefinition tableDefinition = TableDefinition.builder()
                .name("TEST_ENCODE")
                .columns(Arrays.asList(c1, c2))
                .version(1).codecVersion(2)
                .build();

        DingoKeyValueCodec codec = DingoKeyValueCodec.of(1, 1L, tableDefinition);
        Object[] record = {".00012345000098765", 1};
        Object[] expected = {"0.00012345000098765000", 1};
        Assertions.assertArrayEquals(expected, codec.decode(codec.encode(record)));
    }

    @Test
    public void testDecimal10() throws IOException {

        ColumnDefinition c1 = ColumnDefinition.builder().name("col1").type("DECIMAL").primary(0).precision(30).scale(20).build();
        ColumnDefinition c2 = ColumnDefinition.builder().name("col2").type("INT").primary(-1).build();

        TableDefinition tableDefinition = TableDefinition.builder()
                .name("TEST_ENCODE")
                .columns(Arrays.asList(c1, c2))
                .version(1).codecVersion(2)
                .build();

        DingoKeyValueCodec codec = DingoKeyValueCodec.of(1, 1L, tableDefinition);
        Object[] record = {".12345000098765", 1};
        Object[] expected = {"0.12345000098765000000", 1};
        Assertions.assertArrayEquals(expected, codec.decode(codec.encode(record)));
    }

    @Test
    public void testDecimal11() throws IOException {

        ColumnDefinition c1 = ColumnDefinition.builder().name("col1").type("DECIMAL").primary(0).precision(30).scale(5).build();
        ColumnDefinition c2 = ColumnDefinition.builder().name("col2").type("INT").primary(-1).build();

        TableDefinition tableDefinition = TableDefinition.builder()
                .name("TEST_ENCODE")
                .columns(Arrays.asList(c1, c2))
                .version(1).codecVersion(2)
                .build();

        DingoKeyValueCodec codec = DingoKeyValueCodec.of(1, 1L, tableDefinition);
        Object[] record = {"1234500009876.5", 1};
        Object[] expected = {"1234500009876.50000", 1};
        Assertions.assertArrayEquals(expected, codec.decode(codec.encode(record)));
    }

    @Test
    public void testDecimal12() throws IOException {

        ColumnDefinition c1 = ColumnDefinition.builder().name("col1").type("DECIMAL").primary(0).precision(7).scale(3).build();
        ColumnDefinition c2 = ColumnDefinition.builder().name("col2").type("INT").primary(-1).build();

        TableDefinition tableDefinition = TableDefinition.builder()
                .name("TEST_ENCODE")
                .columns(Arrays.asList(c1, c2))
                .version(1).codecVersion(2)
                .build();

        DingoKeyValueCodec codec = DingoKeyValueCodec.of(1, 1L, tableDefinition);
        Object[] record = {"000000000.01", 1};
        Object[] expected = {"0.010", 1};
        Assertions.assertArrayEquals(expected, codec.decode(codec.encode(record)));
    }

    @Test
    public void testDecimal13() throws IOException {

        ColumnDefinition c1 = ColumnDefinition.builder().name("col1").type("DECIMAL").primary(0).precision(10).scale(2).build();
        ColumnDefinition c2 = ColumnDefinition.builder().name("col2").type("INT").primary(-1).build();

        TableDefinition tableDefinition = TableDefinition.builder()
                .name("TEST_ENCODE")
                .columns(Arrays.asList(c1, c2))
                .version(1).codecVersion(2)
                .build();

        DingoKeyValueCodec codec = DingoKeyValueCodec.of(1, 1L, tableDefinition);
        Object[] record = {"123.4", 1};
        Object[] expected = {"123.40", 1};
        Assertions.assertArrayEquals(expected, codec.decode(codec.encode(record)));
    }

    @Test
    public void testDecimal14() throws IOException {

        ColumnDefinition c1 = ColumnDefinition.builder().name("col1").type("DECIMAL").primary(0).precision(1).scale(1).build();
        ColumnDefinition c2 = ColumnDefinition.builder().name("col2").type("INT").primary(-1).build();

        TableDefinition tableDefinition = TableDefinition.builder()
                .name("TEST_ENCODE")
                .columns(Arrays.asList(c1, c2))
                .version(1).codecVersion(2)
                .build();

        DingoKeyValueCodec codec = DingoKeyValueCodec.of(1, 1L, tableDefinition);
        Object[] record = {"0.1", 1};
        Object[] expected = {"0.1", 1};
        Assertions.assertArrayEquals(expected, codec.decode(codec.encode(record)));
    }

    @Test
    public void testDecimal15() throws IOException {

        ColumnDefinition c1 = ColumnDefinition.builder().name("col1").type("DECIMAL").primary(0).precision(2).scale(2).build();
        ColumnDefinition c2 = ColumnDefinition.builder().name("col2").type("INT").primary(-1).build();

        TableDefinition tableDefinition = TableDefinition.builder()
                .name("TEST_ENCODE")
                .columns(Arrays.asList(c1, c2))
                .version(1).codecVersion(2)
                .build();

        DingoKeyValueCodec codec = DingoKeyValueCodec.of(1, 1L, tableDefinition);
        Object[] record = {"0.1", 1};
        Object[] expected = {"0.10", 1};
        Assertions.assertArrayEquals(expected, codec.decode(codec.encode(record)));
    }

    @Test
    public void testDecimal16() throws IOException {

        ColumnDefinition c1 = ColumnDefinition.builder().name("col1").type("DECIMAL").primary(0).precision(3).scale(3).build();
        ColumnDefinition c2 = ColumnDefinition.builder().name("col2").type("INT").primary(-1).build();

        TableDefinition tableDefinition = TableDefinition.builder()
                .name("TEST_ENCODE")
                .columns(Arrays.asList(c1, c2))
                .version(1).codecVersion(2)
                .build();

        DingoKeyValueCodec codec = DingoKeyValueCodec.of(1, 1L, tableDefinition);
        Object[] record = {"0.10", 1};
        Object[] expected = {"0.100", 1};
        Assertions.assertArrayEquals(expected, codec.decode(codec.encode(record)));
    }

    @Test
    public void testDecimal17() throws IOException {

        ColumnDefinition c1 = ColumnDefinition.builder().name("col1").type("DECIMAL").primary(0).precision(3).scale(1).build();
        ColumnDefinition c2 = ColumnDefinition.builder().name("col2").type("INT").primary(-1).build();

        TableDefinition tableDefinition = TableDefinition.builder()
                .name("TEST_ENCODE")
                .columns(Arrays.asList(c1, c2))
                .version(1).codecVersion(2)
                .build();

        DingoKeyValueCodec codec = DingoKeyValueCodec.of(1, 1L, tableDefinition);
        Object[] record = {"0.1", 1};
        Object[] expected = {"0.1", 1};
        Assertions.assertArrayEquals(expected, codec.decode(codec.encode(record)));
    }

    @Test
    public void testDecimal18() throws IOException {

        ColumnDefinition c1 = ColumnDefinition.builder().name("col1").type("DECIMAL").primary(0).precision(32).scale(17).build();
        ColumnDefinition c2 = ColumnDefinition.builder().name("col2").type("INT").primary(-1).build();

        TableDefinition tableDefinition = TableDefinition.builder()
                .name("TEST_ENCODE")
                .columns(Arrays.asList(c1, c2))
                .version(1).codecVersion(2)
                .build();

        DingoKeyValueCodec codec = DingoKeyValueCodec.of(1, 1L, tableDefinition);
        Object[] record = {"0.0000000000001234", 1};
        Object[] expected = {"0.00000000000012340", 1};
        Assertions.assertArrayEquals(expected, codec.decode(codec.encode(record)));
    }

    @Test
    public void testDecimal19() throws IOException {

        ColumnDefinition c1 = ColumnDefinition.builder().name("col1").type("DECIMAL").primary(0).precision(20).scale(20).build();
        ColumnDefinition c2 = ColumnDefinition.builder().name("col2").type("INT").primary(-1).build();

        TableDefinition tableDefinition = TableDefinition.builder()
                .name("TEST_ENCODE")
                .columns(Arrays.asList(c1, c2))
                .version(1).codecVersion(2)
                .build();

        DingoKeyValueCodec codec = DingoKeyValueCodec.of(1, 1L, tableDefinition);
        Object[] record = {"0.0000000000001234", 1};
        Object[] expected = {"0.00000000000012340000", 1};
        Assertions.assertArrayEquals(expected, codec.decode(codec.encode(record)));

        Object[] record1 = {"0.0000000000001234", 1};
        Object[] expected1 = {"0.00000000000012340000", 1};
        Assertions.assertArrayEquals(expected1, codec.decode(codec.encode(record1)));
    }

    @Test
    public void testDecimal20() throws IOException {

        ColumnDefinition c1 = ColumnDefinition.builder().name("col1").type("DECIMAL").primary(0).precision(20).scale(5).build();
        ColumnDefinition c2 = ColumnDefinition.builder().name("col2").type("INT").primary(-1).build();

        TableDefinition tableDefinition = TableDefinition.builder()
                .name("TEST_ENCODE")
                .columns(Arrays.asList(c1, c2))
                .version(1).codecVersion(2)
                .build();

        DingoKeyValueCodec codec = DingoKeyValueCodec.of(1, 1L, tableDefinition);
        Object[] record = {"-12345.6", 1};
        Object[] expected = {"-12345.60000", 1};
        Assertions.assertArrayEquals(expected, codec.decode(codec.encode(record)));

        Object[] record1 = {"123.456e10", 1};
        Object[] expected1 = {"1234560000000.00000", 1};
        Assertions.assertArrayEquals(expected1, codec.decode(codec.encode(record1)));

        Object[] record2 = {"123.456e2", 1};
        Object[] expected2 = {"12345.60000", 1};
        Assertions.assertArrayEquals(expected2, codec.decode(codec.encode(record2)));

        Object[] record3 = {"123.456e-2", 1};
        Object[] expected3 = {"1.23456", 1};
        Assertions.assertArrayEquals(expected3, codec.decode(codec.encode(record3)));

        Object[] record4 = {"12.3e-4", 1};
        Object[] expected4 = {"0.00123", 1};
        Assertions.assertArrayEquals(expected4, codec.decode(codec.encode(record4)));

        Object[] record5 = {"+123.456e10", 1};
        Object[] expected5 = {"1234560000000.00000", 1};
        Assertions.assertArrayEquals(expected5, codec.decode(codec.encode(record5)));

        Object[] record6 = {"+123.456e2", 1};
        Object[] expected6 = {"12345.60000", 1};
        Assertions.assertArrayEquals(expected6, codec.decode(codec.encode(record6)));

        Object[] record7 = {"+123.456e-2", 1};
        Object[] expected7 = {"1.23456", 1};
        Assertions.assertArrayEquals(expected7, codec.decode(codec.encode(record7)));

        Object[] record8 = {"+12.3e-4", 1};
        Object[] expected8 = {"0.00123", 1};
        Assertions.assertArrayEquals(expected8, codec.decode(codec.encode(record8)));

        Object[] record9 = {"-123.456e10", 1};
        Object[] expected9 = {"-1234560000000.00000", 1};
        Assertions.assertArrayEquals(expected9, codec.decode(codec.encode(record9)));

        Object[] record10 = {"-123.456e2", 1};
        Object[] expected10 = {"-12345.60000", 1};
        Assertions.assertArrayEquals(expected10, codec.decode(codec.encode(record10)));

        Object[] record11 = {"-123.456e-2", 1};
        Object[] expected11 = {"-1.23456", 1};
        Assertions.assertArrayEquals(expected11, codec.decode(codec.encode(record11)));

        Object[] record12 = {"-12.3e-4", 1};
        Object[] expected12 = {"-0.00123", 1};
        Assertions.assertArrayEquals(expected12, codec.decode(codec.encode(record12)));

        Object[] record13 = {"-12.3e0", 1};
        Object[] expected13 = {"-12.30000", 1};
        Assertions.assertArrayEquals(expected13, codec.decode(codec.encode(record13)));

        Object[] record14 = {"12.3e0", 1};
        Object[] expected14 = {"12.30000", 1};
        Assertions.assertArrayEquals(expected14, codec.decode(codec.encode(record14)));
    }

    @Test
    public void testAdd() throws IOException {

        ColumnDefinition c1 = ColumnDefinition.builder().name("col1").type("VARCHAR").primary(0).build();
        ColumnDefinition c2 = ColumnDefinition.builder().name("col2").type("BOOL").primary(1).build();
        ColumnDefinition c3 = ColumnDefinition.builder().name("col3").type("DOUBLE").scale(1).primary(-1).build();
        ColumnDefinition c4 = ColumnDefinition.builder().name("col4").type("LONG").primary(-1).build();

        TableDefinition beforeTable = TableDefinition.builder()
            .name("TEST_BEFORE")
            .columns(Arrays.asList(c1, c2, c3))
            .version(1).codecVersion(2)
            .build();

        TableDefinition afterTable = TableDefinition.builder()
            .name("TEST_AFTER")
            .columns(Arrays.asList(c1, c2, c3, c4))
            .version(2).codecVersion(2)
            .build();

        Object[] beforeRecord = {"id1_1", true, 1.02};
        Object[] afterRecord = {"id1_1", true, 1.02, 123L};

        DingoKeyValueCodec beforeCodec = DingoKeyValueCodec.of(1L, beforeTable);
        DingoKeyValueCodec afterCodec = DingoKeyValueCodec.of(1L, afterTable);

        KeyValue beforeKV = beforeCodec.encode(beforeRecord);
        KeyValue afterKV = afterCodec.encode(afterRecord);

        Assertions.assertArrayEquals(beforeRecord, beforeCodec.decode(beforeKV));
        Assertions.assertArrayEquals(afterRecord, afterCodec.decode(afterKV));

        Assertions.assertArrayEquals(new Object[] {"id1_1", true, 1.02, null}, afterCodec.decode(beforeKV));
        Assertions.assertThrows(RuntimeException.class, () -> beforeCodec.decode(afterKV));

    }


    @Test
    public void testRemove() throws IOException {

        ColumnDefinition c1 = ColumnDefinition.builder().name("col1").type("VARCHAR").primary(0).build();
        ColumnDefinition c2 = ColumnDefinition.builder().name("col2").type("BOOL").primary(1).build();
        ColumnDefinition c3 = ColumnDefinition.builder().name("col3").type("DOUBLE").scale(1).primary(-1).build();
        ColumnDefinition c4 = ColumnDefinition.builder().name("col4").type("LONG").primary(-1).build();

        TableDefinition beforeTable = TableDefinition.builder()
            .name("TEST_BEFORE")
            .columns(Arrays.asList(c1, c2, c3, c4))
            .version(1).codecVersion(2)
            .build();

        TableDefinition afterTable = TableDefinition.builder()
            .name("TEST_AFTER")
            .columns(Arrays.asList(c1, c2, c3))
            .version(2).codecVersion(2)
            .build();

        Object[] beforeRecord = {"id1_1", true, 1.02, 123L};
        Object[] afterRecord = {"id1_1", true, 1.02};

        DingoKeyValueCodec beforeCodec = DingoKeyValueCodec.of(1L, beforeTable);
        DingoKeyValueCodec afterCodec = DingoKeyValueCodec.of(1L, afterTable);

        KeyValue beforeKV = beforeCodec.encode(beforeRecord);
        KeyValue afterKV = afterCodec.encode(afterRecord);

        Assertions.assertArrayEquals(beforeRecord, beforeCodec.decode(beforeKV));
        Assertions.assertArrayEquals(afterRecord, afterCodec.decode(afterKV));

        Assertions.assertArrayEquals(new Object[] {"id1_1", true, 1.02}, afterCodec.decode(beforeKV));
        Assertions.assertThrows(RuntimeException.class, () -> beforeCodec.decode(afterKV));

    }



}
