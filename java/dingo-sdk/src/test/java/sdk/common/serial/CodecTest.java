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
            .version(1)
            .build();

        DingoKeyValueCodec codec = DingoKeyValueCodec.of(1, 1L, tableDefinition);
        Object[] record = {"id1_1", true, 1.02, 123L};
        Assertions.assertArrayEquals(record, codec.decode(codec.encode(record)));

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
            .version(1)
            .build();

        TableDefinition afterTable = TableDefinition.builder()
            .name("TEST_AFTER")
            .columns(Arrays.asList(c1, c2, c3, c4))
            .version(2)
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
            .version(1)
            .build();

        TableDefinition afterTable = TableDefinition.builder()
            .name("TEST_AFTER")
            .columns(Arrays.asList(c1, c2, c3))
            .version(2)
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
