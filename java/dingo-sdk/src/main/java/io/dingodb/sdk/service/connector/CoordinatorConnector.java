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

import io.dingodb.common.Common;
import io.dingodb.coordinator.Coordinator;
import io.dingodb.coordinator.CoordinatorServiceGrpc;
import io.dingodb.meta.MetaServiceGrpc;
import io.dingodb.sdk.common.Location;
import io.dingodb.sdk.common.utils.GrpcConnection;
import io.dingodb.sdk.common.utils.Optional;
import io.grpc.ConnectivityState;
import lombok.extern.slf4j.Slf4j;

import javax.activation.UnsupportedDataTypeException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
public class CoordinatorConnector extends ServiceConnector<MetaServiceGrpc.MetaServiceBlockingStub> {

    public CoordinatorConnector(List<Location> locations) {
        super(null, new HashSet<>(locations));
    }

    public static CoordinatorConnector getCoordinatorConnector(String target) {
        return Optional.ofNullable(target.split(","))
            .map(Arrays::stream)
            .map(ss -> ss
                .map(s -> s.split(":"))
                .map(__ -> new Location(__[0], Integer.parseInt(__[1])))
                .collect(Collectors.toList()))
            .map(CoordinatorConnector::new).orNull();
    }

    @Override
    public void refresh() {
        try {
            connectionLeader();
        } catch (UnsupportedDataTypeException e) {
            throw new RuntimeException(e);
        }
        if (channel != null) {
            onChannelConnected();
        }
    }

    public void connectionLeader() throws UnsupportedDataTypeException {
        for (Location location : locations) {
            String target = location.getHost() + ":" + location.getPort();
            channel = GrpcConnection.newChannel(target);
            ConnectivityState state = channel.getState(true);
            if (state == ConnectivityState.CONNECTING || state == ConnectivityState.READY || state == ConnectivityState.IDLE) {
                CoordinatorServiceGrpc.CoordinatorServiceBlockingStub blockingStub =
                        (CoordinatorServiceGrpc.CoordinatorServiceBlockingStub) GrpcConnection.newBlockingStub(
                                channel, GrpcConnection.GrpcType.COORDINATOR);

                Coordinator.GetCoordinatorMapRequest request = Coordinator.GetCoordinatorMapRequest.newBuilder().setClusterId(0).build();
                Coordinator.GetCoordinatorMapResponse response = blockingStub.getCoordinatorMap(request);

                Common.Location leaderLocation = response.getLeaderLocation();
                if (!leaderLocation.getHost().isEmpty()) {
                    GrpcConnection.shutdownManagedChannel(channel, log);
                    target = leaderLocation.getHost() + ":" + leaderLocation.getPort();
                    channel = GrpcConnection.newChannel(target);
                    this.blockingStub = (MetaServiceGrpc.MetaServiceBlockingStub)
                            GrpcConnection.newBlockingStub(channel, GrpcConnection.GrpcType.META);
                    return;
                }
            }
        }
    }
}
