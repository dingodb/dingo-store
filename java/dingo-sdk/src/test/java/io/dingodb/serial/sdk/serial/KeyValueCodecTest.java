package io.dingodb.serial.sdk.serial;

import io.dingodb.sdk.common.KeyValue;
import io.dingodb.sdk.common.codec.DingoKeyValueCodec;
import io.dingodb.sdk.common.codec.KeyValueCodec;
import io.dingodb.sdk.common.table.ColumnDefinition;
import io.dingodb.sdk.common.table.TableDefinition;

import java.util.Arrays;

import static io.dingodb.common.Common.Engine.ENG_ROCKSDB;

public class KeyValueCodecTest {
    public static void main(String[] args) throws Exception {
        ColumnDefinition c1 = new ColumnDefinition("id", "integer", "", 0, 0, false, 0, "");
        ColumnDefinition c2 = new ColumnDefinition("name", "varchar", "", 0, 0, false, -1, "");
        TableDefinition tableDefinition = new TableDefinition("test2", Arrays.asList(c1, c2), 1, 0, null, ENG_ROCKSDB.name(), null);

        KeyValueCodec kvCodec = new DingoKeyValueCodec(tableDefinition.getDingoType(), tableDefinition.getKeyMapping());

        Object[] record = new Object[] {1, "a"};

        KeyValue kv = kvCodec.encode(record);
        byte[] key = kvCodec.encodeKey(record);

        Object drocer = kvCodec.decode(kv);
        System.out.println(drocer);

    }
}
