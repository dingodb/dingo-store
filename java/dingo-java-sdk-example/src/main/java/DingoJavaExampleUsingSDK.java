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
        if (args.length < 4) {
            System.out.println("java -cp dingo-java-sdk-example-0.6.0-SNAPSHOT.jar DingoJavaExampleUsingSDK 192.168.1.201:22001 huzx 1000000000 true xxxxxxxxxx");
            System.out.println("\t=> Args1: CoordinatorConnection(IP:PORT)");
            System.out.println("\t=> Args2: TableName(ABC)");
            System.out.println("\t=> Args3: RecordCnt(1000)");
            System.out.println("\t=> Args4: IsReCreateTable(true: will drop table and create; else false)");
            System.out.println("\t=> Args5: KeyPrefix(like xxxxx)");
            System.exit(-1);
        }
        String coordinatorAddress = args[0];
        String tableName = args[1];
        Long  recordCnt = Long.parseLong(args[2]);
        boolean isReCreateTable = args[3].equals("true") ? true : false;
        String keyPrefix = "";
        if (args.length >= 5) {
            keyPrefix = args[4];
        }

        System.out.println("1=>" + coordinatorAddress + ",2=>" + tableName + ",3=>" + recordCnt + ",4=>" + isReCreateTable);

        DingoClient dingoClient = new DingoClient(coordinatorAddress, 10);
        dingoClient.open();

        ColumnDefinition c1 = new ColumnDefinition("id", "varchar", "", 0, 0, false, 0, "");
        ColumnDefinition c2 = new ColumnDefinition("name", "varchar", "", 0, 0, false, -1, "");

        PartitionDetailDefinition detailDefinition = new PartitionDetailDefinition(null, null, Arrays.asList(new Object[]{"1"}));
        PartitionRule partitionRule = new PartitionRule(null, null, Arrays.asList(detailDefinition));

        TableDefinition tableDefinition = new TableDefinition(tableName,
                Arrays.asList(c1, c2),
                1,
                0,
                null,
                Common.Engine.ENG_ROCKSDB.name(),
                null);

        if (isReCreateTable) {
            try {
                dingoClient.dropTable(tableName);
            } catch(Exception ex) {
            }
            boolean isSuccess = dingoClient.createTable(tableDefinition);
            System.out.println("drop table and create table:" + isSuccess);
            try {
                Thread.sleep(12000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        Long test_count = recordCnt;
        for (Long i = 0L; i < test_count; i++) {
            Map<String, Object> map = Maps.newLinkedHashMap();
            map.put(c1.getName(), keyPrefix + i);
            map.put(c2.getName(), keyPrefix + "=zhangsan=>" + i);
            boolean test = dingoClient.upsert(tableName, Collections.singletonList(new Record(c1.getName(), map)));
            if (i % 1000 == 0) {
                System.out.println("Write key: " + i);
            }
        }

        for (Long i = 0L; i < test_count; i++) {
            Record record = dingoClient.get(tableName, new Key(Arrays.asList(Value.get(keyPrefix + i), Value.get(""))));
            System.out.println(record);
        }

        dingoClient.close();
    }
}

