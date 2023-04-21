package io.dingodb.client.operation;

import io.dingodb.client.Key;
import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public class OpKeyRange {
    public final Key start;
    public final Key end;
    public final boolean withStart;
    public final boolean withEnd;
}
