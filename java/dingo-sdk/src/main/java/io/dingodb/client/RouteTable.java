/*
 * Copyright 2021 DataCanvas
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
    private NavigableMap<ByteArrayUtils.ComparableByteArray, Meta.RangeDistribution> partRange;
    private PartitionStrategy<ByteArrayUtils.ComparableByteArray> partitionStrategy;

    public StoreServiceClient getLeaderStoreService(String leaderAddress) {
        return new StoreServiceClient(leaderAddress);
    }

    public Meta.RangeDistribution getPartByKey(byte[] keyInBytes) {
        ByteArrayUtils.ComparableByteArray byteArray = partitionStrategy.calcPartId(keyInBytes);
        return partRange.get(byteArray);
    }
}
