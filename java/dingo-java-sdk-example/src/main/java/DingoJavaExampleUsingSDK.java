import io.dingodb.common.Common;
import io.dingodb.sdk.common.DingoCommonId;
import io.dingodb.sdk.common.KeyValue;
import io.dingodb.sdk.common.codec.DingoKeyValueCodec;
import io.dingodb.sdk.common.table.ColumnDefinition;
import io.dingodb.sdk.common.table.RangeDistribution;
import io.dingodb.sdk.common.table.Table;
import io.dingodb.sdk.common.table.TableDefinition;
import io.dingodb.sdk.common.utils.ByteArrayUtils.ComparableByteArray;
import io.dingodb.sdk.service.meta.MetaServiceClient;
import io.dingodb.sdk.service.store.StoreServiceClient;

import java.io.IOException;
import java.util.Arrays;
import java.util.NavigableMap;

public class DingoJavaExampleUsingSDK {

    private static MetaServiceClient metaService;
    private static StoreServiceClient storeService;

    public static void main(String[] args) throws IOException {
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
        String keyPrefix = "id-";
        if (args.length >= 5) {
            keyPrefix = args[4];
        }

        System.out.println("1=>" + coordinatorAddress + ",2=>" + tableName + ",3=>" + recordCnt + ",4=>" + isReCreateTable);

        metaService = new MetaServiceClient(coordinatorAddress).getSubMetaService("dingo");
        DingoCommonId tableId = metaService.getTableId(tableName);
        if (tableId == null) {
            createTable(tableName);
        } else if (isReCreateTable) {
            metaService.dropTable(tableName);
            createTable(tableName);
        }
        tableId = metaService.getTableId(tableName);
        Table table = metaService.getTableDefinition(tableName);

        DingoKeyValueCodec codec = DingoKeyValueCodec.of(tableId.entityId(), table);

        storeService = new StoreServiceClient(metaService);

        NavigableMap<ComparableByteArray, RangeDistribution> distributions = metaService.getRangeDistribution(tableId);

        for (Long i = 0L; i < recordCnt; i++) {
            Object[] record = new Object[] {keyPrefix + i, "name-" + i};
            KeyValue keyValue = codec.encode(record);
            DingoCommonId regionId = distributions.floorEntry(new ComparableByteArray(keyValue.getKey())).getValue().getId();
            boolean success = storeService.kvPut(tableId, regionId, keyValue);
            System.out.printf("Write key id: %s, result: %s\n", i, success);
        }

        for (Long i = 0L; i < recordCnt; i++) {
            Object[] record = new Object[] {keyPrefix + i};
            byte[] key = codec.encodeKey(record);
            DingoCommonId regionId = distributions.floorEntry(new ComparableByteArray(key)).getValue().getId();
            byte[] result = storeService.kvGet(tableId, regionId, key);
            System.out.printf(
                "Get key id: %s, result: %s\n",
                i, Arrays.toString(codec.decode(new KeyValue(key, result)))
            );
        }

    }

    private static void createTable(String tableName) {
        ColumnDefinition c1 = ColumnDefinition.builder()
            .name("id").type("varchar").nullable(false).primary(0)
            .build();
        ColumnDefinition c2 = ColumnDefinition.builder()
            .name("name").type("varchar").nullable(false).primary(-1)
            .build();

        TableDefinition tableDefinition = TableDefinition.builder()
            .name(tableName)
            .columns(Arrays.asList(c1, c2))
            .version(1)
            .engine(Common.Engine.ENG_ROCKSDB.name())
            .replica(3)
            .build();

        metaService.createTable(tableName, tableDefinition);
    }

}

