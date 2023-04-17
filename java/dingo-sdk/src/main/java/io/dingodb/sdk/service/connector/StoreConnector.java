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

package io.dingodb.sdk.service.connector;

import io.dingodb.sdk.common.DingoClientException;
import io.dingodb.sdk.common.DingoCommonId;
import io.dingodb.sdk.common.Location;
import io.dingodb.sdk.common.utils.GrpcConnection;
import io.dingodb.sdk.service.meta.MetaClient;
import io.dingodb.store.StoreServiceGrpc;

import javax.activation.UnsupportedDataTypeException;
import java.util.HashSet;
import java.util.List;

public class StoreConnector extends ServiceConnector<StoreServiceGrpc.StoreServiceBlockingStub> {

    private MetaClient metaClient;
    private DingoCommonId tableId;

    public StoreConnector(MetaClient metaClient, DingoCommonId tableId, Location leader, List<Location> locations) {
        super(leader, new HashSet<>(locations));
        this.tableId = tableId;
        this.metaClient = metaClient;
    }

    @Override
    public void refresh() {
        if (leader != null) {
            getStoreStub(leader);
        } else {
            for (Location location : locations) {
                getStoreStub(location);
            }
        }
        if (channel != null) {
            onChannelConnected();
        }
    }

    private void getStoreStub(Location location) {
        if (location.getHost().isEmpty() || location.getPort() == 0) {
            throw new DingoClientException(-1, "not found leader");
        }
        channel = GrpcConnection.newChannel(location.getHost() + ":" + location.getPort());
        try {
            blockingStub = (StoreServiceGrpc.StoreServiceBlockingStub)
                    GrpcConnection.newBlockingStub(channel, GrpcConnection.GrpcType.STORE);
        } catch (UnsupportedDataTypeException e) {
            throw new RuntimeException(e);
        }
    }
}
