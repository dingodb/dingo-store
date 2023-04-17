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

import io.dingodb.sdk.common.Location;
import io.dingodb.sdk.common.utils.GrpcConnection;
import io.grpc.ConnectivityState;
import io.grpc.ManagedChannel;
import io.grpc.stub.AbstractBlockingStub;
import lombok.extern.slf4j.Slf4j;

import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

@Slf4j
public abstract class ServiceConnector<S extends AbstractBlockingStub<S>> {

    protected ManagedChannel channel;
    protected S blockingStub;

    protected Location leader;
    protected Set<Location> locations = new CopyOnWriteArraySet<>();

    public ServiceConnector(Location leader, Set<Location> locations) {
        this.leader = leader;
        this.locations.addAll(locations);
        refresh();
    }

    public S getBlockingStub() {
        if (channel == null || blockingStub == null) {
            return null;
        }
        if (channel.getState(true) == ConnectivityState.TRANSIENT_FAILURE) {
            refresh();
        }
        return blockingStub;
    }

    public abstract void refresh();

    protected synchronized void onChannelConnected() {
        channel.notifyWhenStateChanged(ConnectivityState.IDLE, this::refresh);
        // channel.notifyWhenStateChanged(ConnectivityState.TRANSIENT_FAILURE, this::refresh);
    }

    public Set<Location> getLocations() {
        return locations;
    }

    public void shutdown() {
        GrpcConnection.shutdownManagedChannel(channel, log);
    }
}
