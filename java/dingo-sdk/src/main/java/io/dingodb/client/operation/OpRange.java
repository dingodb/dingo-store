package io.dingodb.client.operation;

import io.dingodb.sdk.common.Range;
import lombok.Getter;
import lombok.experimental.Delegate;

@Getter
public class OpRange {

    @Delegate
    public final Range range;
    public final boolean withStart;
    public final boolean withEnd;

    public OpRange(byte[] startKey, byte[] endKey, boolean withStart, boolean withEnd) {
        this.range = new Range(startKey, endKey);
        this.withStart = withStart;
        this.withEnd = withEnd;
    }

    public OpRange(Range range, boolean withStart, boolean withEnd) {
        this.range = range;
        this.withStart = withStart;
        this.withEnd = withEnd;
    }
}
