package io.dingodb.sdk.common;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;

import java.util.function.Function;

@EqualsAndHashCode(onlyExplicitlyIncluded = true)
public class AutoIncrement {

    @AllArgsConstructor
    public static class Increment {
        public final long limit;
        public final long inc;
    }

    @EqualsAndHashCode.Include
    public final DingoCommonId tableId;
    public final int increment;
    public final int offset;

    private final Function<DingoCommonId, Increment> fetcher;

    private long limit = 0;
    private long inc = 0;

    public AutoIncrement(DingoCommonId tableId, int increment, int offset, Function<DingoCommonId, Increment> fetcher) {
        this.tableId = tableId;
        this.increment = increment;
        this.offset = offset;
        this.fetcher = fetcher;
        fetch();
    }

    public long current() {
        return inc;
    }

    public synchronized long inc() {
        if (inc >= limit) {
            fetch();
        }
        return inc++;
    }

    private void fetch() {
        Increment increment = fetcher.apply(tableId);
        if (increment.inc >= increment.limit) {
            throw new RuntimeException("Fetch zero increment, table id: {}" + tableId);
        }
        this.inc = increment.inc;
        this.limit = increment.limit;
    }

}
