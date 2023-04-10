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
import io.dingodb.sdk.common.utils.GrpcConnection;
import io.dingodb.meta.MetaServiceGrpc;
import io.grpc.ConnectivityState;
import io.grpc.ManagedChannel;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
public class ServiceConnector {

    private ManagedChannel channel;
    @Getter
    private MetaServiceGrpc.MetaServiceBlockingStub metaBlockingStub;

    private Set<Common.Location> locations = new CopyOnWriteArraySet<>();

    private AtomicBoolean isShutdown = new AtomicBoolean(false);

    public ServiceConnector(Set<Common.Location> locations) {
        this.locations.addAll(locations);
        initConnection();
    }

    public Set<Common.Location> getLocations() {
        return locations;
    }

    public Common.Location getLeader() {
        for (Common.Location location : locations) {
            String target = location.getHost() + ":" + location.getPort();
            channel = GrpcConnection.newChannel(target);
            ConnectivityState state = channel.getState(true);
            if (state == ConnectivityState.CONNECTING || state == ConnectivityState.READY || state == ConnectivityState.IDLE) {
                CoordinatorServiceGrpc.CoordinatorServiceBlockingStub blockingStub =
                        CoordinatorServiceGrpc.newBlockingStub(channel);
                Coordinator.GetCoordinatorMapResponse response = blockingStub.getCoordinatorMap(
                        Coordinator.GetCoordinatorMapRequest.newBuilder().setClusterId(0).build());

                Common.Location leaderLocation = response.getLeaderLocation();
                GrpcConnection.shutdownManagedChannel(channel, log);
                if (!leaderLocation.getHost().isEmpty()) {
                    target = leaderLocation.getHost() + ":" + leaderLocation.getPort();
                    channel = GrpcConnection.newChannel(target);
                    metaBlockingStub = MetaServiceGrpc.newBlockingStub(channel);
                    return leaderLocation;
                }
            }
        }
        return null;
    }

    public void initConnection() {
        connection();
        channel.notifyWhenStateChanged(ConnectivityState.TRANSIENT_FAILURE, this::connection);
        channel.notifyWhenStateChanged(ConnectivityState.SHUTDOWN, this::connection);
    }

    private void connection() {
        if (isShutdown.get()) {
            return;
        }
        if (channel != null) {
            return;
        }
        getLeader();
    }

    public void shutdown() {
        GrpcConnection.shutdownManagedChannel(channel, log);
        isShutdown.set(true);
    }
}
