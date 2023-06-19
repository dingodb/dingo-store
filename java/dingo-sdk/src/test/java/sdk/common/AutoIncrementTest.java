package sdk.common;

import io.dingodb.sdk.common.AutoIncrement;
import io.dingodb.sdk.common.DingoCommonId;
import io.dingodb.sdk.common.SDKCommonId;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.function.Function;

public class AutoIncrementTest {

    private static final SDKCommonId TABLE_ID = new SDKCommonId(DingoCommonId.Type.ENTITY_TYPE_TABLE, 0, 0);

    private static class IncFunction implements Function<DingoCommonId, AutoIncrement.Increment> {
        long inc;
        long limit;

        @Override
        public AutoIncrement.Increment apply(DingoCommonId dingoCommonId) {
            return new AutoIncrement.Increment(limit, inc);
        }
    }

    @Test
    public void testIncrement1Offset1() {
        IncFunction incFunction = new IncFunction();
        incFunction.inc = 1;
        incFunction.limit = 3;
        AutoIncrement autoIncrement = new AutoIncrement(TABLE_ID, 1, 1, incFunction);
        Assertions.assertEquals(1, autoIncrement.inc());
        Assertions.assertEquals(2, autoIncrement.inc());
        incFunction.inc = 3;
        incFunction.limit = 5;
        Assertions.assertEquals(3, autoIncrement.inc());
        Assertions.assertEquals(4, autoIncrement.inc());
    }

    @Test
    public void testIncrement2Offset1() {
        IncFunction incFunction = new IncFunction();
        incFunction.inc = 1;
        incFunction.limit = 6;
        AutoIncrement autoIncrement = new AutoIncrement(TABLE_ID, 2, 1, incFunction);
        Assertions.assertEquals(1, autoIncrement.inc());
        Assertions.assertEquals(3, autoIncrement.inc());
        Assertions.assertEquals(5, autoIncrement.inc());
        incFunction.inc = 7;
        incFunction.limit = 10;
        Assertions.assertEquals(7, autoIncrement.inc());
    }

    @Test
    public void testIncrement2Offset2() {
        IncFunction incFunction = new IncFunction();
        incFunction.inc = 1;
        incFunction.limit = 6;
        AutoIncrement autoIncrement = new AutoIncrement(TABLE_ID, 2, 2, incFunction);
        Assertions.assertEquals(2, autoIncrement.inc());
        Assertions.assertEquals(4, autoIncrement.inc());
        incFunction.inc = 6;
        incFunction.limit = 11;
        Assertions.assertEquals(6, autoIncrement.inc());
        Assertions.assertEquals(8, autoIncrement.inc());
        incFunction.inc = 11;
        incFunction.limit = 15;
        Assertions.assertEquals(10, autoIncrement.inc());
        Assertions.assertEquals(12, autoIncrement.inc());
        Assertions.assertEquals(14, autoIncrement.inc());
    }

}
