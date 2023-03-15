import io.dingodb.DingoClient;
import io.dingodb.client.Result;
import io.dingodb.common.Common;
import io.dingodb.sdk.common.partition.PartitionDetailDefinition;
import io.dingodb.sdk.common.partition.PartitionRule;
import io.dingodb.sdk.common.table.ColumnDefinition;
import io.dingodb.sdk.common.table.TableDefinition;

import java.util.Arrays;
import java.util.Collections;

public class DingoJavaExampleUsingSDK {

    public static void main(String[] args) {

        DingoClient dingoClient = new DingoClient("192.168.1.201:22001", 10);
        dingoClient.open();

        String tableName = "test";

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

        boolean test = dingoClient.upsert(tableName, Collections.singletonList(new Object[]{1, null}), Collections.singletonList(new Object[]{1, "zhangsan"}));
        System.out.println(test);

        Result result = dingoClient.get(tableName, Collections.singletonList(new Object[]{1, null}));

        for (Object[] value : result.getValues()) {
            System.out.println(Arrays.toString(value));
        }
    }
}
