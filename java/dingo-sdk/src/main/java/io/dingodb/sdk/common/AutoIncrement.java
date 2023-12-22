package io.dingodb.sdk.common;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;

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

    @Getter
    private long limit = 0;
    private volatile long inc = 0;

    public AutoIncrement(DingoCommonId tableId, int increment, int offset, Function<DingoCommonId, Increment> fetcher) {
        this.tableId = tableId;
        this.increment = increment;
        this.offset = offset;
        this.fetcher = fetcher;
    }

    public long current() {
        return inc;
    }

    public synchronized long inc() {
        long current = inc;
        if (current >= limit) {
            current = fetch();
        }
        inc += increment;
        return current;
    }

    public synchronized void inc(long targetInc) {
        if (targetInc > inc) {
            inc = targetInc;
        }
    }

    private long fetch() {
        Increment increment = fetcher.apply(tableId);
        if ((increment.inc + this.increment) >= increment.limit) {
            throw new RuntimeException("Fetch zero increment, table id: {}" + tableId);
        }
        long incTmp;
        if (increment.inc % this.offset != 0) {
            incTmp = increment.inc + this.offset - increment.inc % this.offset;
        } else {
            incTmp = increment.inc;
        }
        if (incTmp > this.inc) {
            this.inc = incTmp;
        }
        this.limit = increment.limit;
        return inc;
    }

}
