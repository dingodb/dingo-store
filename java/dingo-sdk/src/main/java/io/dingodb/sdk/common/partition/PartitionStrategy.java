/*
 * Copyright 2021, Zetyun DataPortal All rights reserved.
 */

package io.dingodb.sdk.common.partition;

import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public abstract class PartitionStrategy<I> {

    public abstract int getPartNum();

    // Should be `String` for json serialization.
    public abstract I calcPartId(final Object @NonNull [] keyTuple);

    public abstract I calcPartId(final byte @NonNull [] keyBytes);

    public abstract Map<byte[], byte[]> calcPartitionRange(
            final byte @NonNull [] startKey,
            final byte @NonNull [] endKey,
            boolean includeEnd
    );

    public abstract Map<byte[], byte[]> calcPartitionPrefixRange(
            final byte @NonNull [] startKey,
            final byte @NonNull [] endKey,
            boolean includeEnd,
            boolean prefixRange
    );

    public abstract Map<byte[], byte[]> calcPartitionByPrefix(final byte @NonNull [] prefix);

    public Map<I, List<Object[]>> partKeyTuples(
            final @NonNull Collection<Object[]> keyTuples
    ) {
        Map<I, List<Object[]>> map = new LinkedHashMap<>(getPartNum());
        for (Object[] tuple : keyTuples) {
            I partId = calcPartId(tuple);
            map.putIfAbsent(partId, new LinkedList<>());
            map.get(partId).add(tuple);
        }
        return map;
    }

}
