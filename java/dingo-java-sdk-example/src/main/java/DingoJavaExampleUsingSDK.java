import com.google.common.collect.Maps;
import io.dingodb.DingoClient;
import io.dingodb.client.Key;
import io.dingodb.client.Record;
import io.dingodb.client.Value;
import io.dingodb.common.Common;
import io.dingodb.sdk.common.partition.PartitionDetailDefinition;
import io.dingodb.sdk.common.partition.PartitionRule;
import io.dingodb.sdk.common.table.ColumnDefinition;
import io.dingodb.sdk.common.table.TableDefinition;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

public class DingoJavaExampleUsingSDK {

    public static void main(String[] args) {

        DingoClient dingoClient = new DingoClient("192.168.1.201:22001", 10);
        dingoClient.open();

        String tableName = "testChannel";

        boolean test1 = dingoClient.dropTable(tableName);
        System.out.println(test1);

        ColumnDefinition c1 = new ColumnDefinition("id", "integer", "", 0, 0, false, 0, "");
        ColumnDefinition c2 = new ColumnDefinition("name", "varchar", "", 0, 0, false, -1, "");

        PartitionDetailDefinition detailDefinition = new PartitionDetailDefinition(null, null, Arrays.asList(new Object[]{1}));
        PartitionRule partitionRule = new PartitionRule(null, null, Arrays.asList(detailDefinition));

        TableDefinition tableDefinition = new TableDefinition(tableName,
                Arrays.asList(c1, c2),
                1,
                0,
                partitionRule,
                Common.Engine.ENG_ROCKSDB.name(),
                null);
        boolean isSuccess = dingoClient.createTable(tableDefinition);

        System.out.println(isSuccess);

        try {
            Thread.sleep(20000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        for (int i = 0; i < 1; i++) {
            Map<String, Object> map = Maps.newLinkedHashMap();
            map.put(c1.getName(), i);
            map.put(c2.getName(), "zhangsan" + i);
            boolean test = dingoClient.upsert(tableName, Collections.singletonList(new Record(c1.getName(), map)));
            System.out.println(test);
        }

        Record record = dingoClient.get(tableName, new Key(Arrays.asList(Value.get(0), Value.get(""))));
        System.out.println(record);

        dingoClient.close();
    }
}
