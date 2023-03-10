/*
 * Copyright 2021, Zetyun DataPortal All rights reserved.
 */

package io.dingodb.client;

import io.dingodb.sdk.common.codec.KeyValueCodec;
import io.dingodb.sdk.service.store.StoreServiceClient;
import io.dingodb.sdk.common.utils.ByteArrayUtils;
import io.dingodb.sdk.common.partition.PartitionStrategy;
import io.dingodb.meta.Meta;
import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.NavigableMap;

@AllArgsConstructor
public class RouteTable {

    private Meta.DingoCommonId tableId;

    @Getter
    private KeyValueCodec codec;
    private NavigableMap<ByteArrayUtils.ComparableByteArray, Meta.Part> partRange;
    private PartitionStrategy<ByteArrayUtils.ComparableByteArray> partitionStrategy;

    public StoreServiceClient getLeaderStoreService(String leaderAddress) {
        return new StoreServiceClient(leaderAddress);
    }

    public Meta.Part getPartByKey(byte[] keyInBytes) {
        ByteArrayUtils.ComparableByteArray byteArray = partitionStrategy.calcPartId(keyInBytes);
        return partRange.get(byteArray);
    }
}
