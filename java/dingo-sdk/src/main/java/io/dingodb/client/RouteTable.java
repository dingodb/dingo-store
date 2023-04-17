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

import io.dingodb.sdk.common.DingoClientException;
import io.dingodb.sdk.common.DingoCommonId;
import io.dingodb.sdk.common.codec.KeyValueCodec;
import io.dingodb.sdk.common.table.RangeDistribution;
import io.dingodb.sdk.service.meta.MetaClient;
import io.dingodb.sdk.service.store.StoreServiceClient;
import io.dingodb.sdk.common.utils.ByteArrayUtils;
import io.dingodb.sdk.common.partition.PartitionStrategy;
import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.NavigableMap;

@AllArgsConstructor
public class RouteTable {

    @Getter
    private DingoCommonId tableId;

    @Getter
    private KeyValueCodec codec;
    private NavigableMap<ByteArrayUtils.ComparableByteArray, RangeDistribution> partRange;
    private PartitionStrategy<ByteArrayUtils.ComparableByteArray> partitionStrategy;

    public StoreServiceClient getLeaderStoreService(MetaClient metaClient) {
        return new StoreServiceClient(metaClient);
    }

    public RangeDistribution getPartByKey(byte[] keyInBytes) {
        if (partRange == null) {
            throw new DingoClientException("The tableRange is empty");
        }
        ByteArrayUtils.ComparableByteArray byteArray = partitionStrategy.calcPartId(keyInBytes);
        return partRange.get(byteArray);
    }
}
