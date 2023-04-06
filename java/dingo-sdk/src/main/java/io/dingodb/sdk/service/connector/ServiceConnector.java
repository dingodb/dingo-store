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
import io.grpc.ManagedChannel;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.TimeUnit;

@Slf4j
public class ServiceConnector {

    private String target;
    private ManagedChannel channel;
    @Getter
    private MetaServiceGrpc.MetaServiceBlockingStub metaBlockingStub;

    public ServiceConnector(String target) {
        this.target = target;
    }

    public void initConnection() {
        channel = GrpcConnection.newChannel(target);
        CoordinatorServiceGrpc.CoordinatorServiceBlockingStub blockingStub =
                CoordinatorServiceGrpc.newBlockingStub(channel);
        Coordinator.GetCoordinatorMapResponse response = blockingStub.getCoordinatorMap(
                Coordinator.GetCoordinatorMapRequest.newBuilder().setClusterId(0).build());

        Common.Location leaderLocation = response.getLeaderLocation();
        if (!leaderLocation.getHost().isEmpty()) {
            GrpcConnection.shutdownManagedChannel(channel, log);
            target = leaderLocation.getHost() + ":" + leaderLocation.getPort();
            channel = GrpcConnection.newChannel(target);
            metaBlockingStub = MetaServiceGrpc.newBlockingStub(channel);
        }
    }

    public void shutdown() {
        GrpcConnection.shutdownManagedChannel(channel, log);
    }
}
