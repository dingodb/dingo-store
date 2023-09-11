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
import io.dingodb.sdk.common.Location;
import io.grpc.ManagedChannel;

import java.util.Set;
import java.util.concurrent.TimeUnit;

public class CoordinatorServiceConnector extends ServiceConnector<CoordinatorServiceGrpc.CoordinatorServiceBlockingStub> {

    private final static Coordinator.GetCoordinatorMapRequest getCoordinatorMapRequest =
            Coordinator.GetCoordinatorMapRequest.newBuilder().setClusterId(0).build();

    public CoordinatorServiceConnector(Set<Location> locations) {
        super(locations);
    }

    public CoordinatorServiceConnector(String locations) {
        super(locations);
    }

    protected Coordinator.GetCoordinatorMapResponse getCoordinatorMap() {
        try {
            return exec(stub -> stub.getCoordinatorMap(getCoordinatorMapRequest), 1);
        } catch (Exception e) {
            return null;
        }
    }

    @Override
    protected ManagedChannel transformToLeaderChannel(ManagedChannel channel) {
        Common.Location leaderLocation = newStub(channel)
            .withDeadlineAfter(1, TimeUnit.SECONDS)
            .getCoordinatorMap(getCoordinatorMapRequest)
            .getLeaderLocation();
        if (!leaderLocation.getHost().isEmpty()) {
            return newChannel(leaderLocation.getHost(), leaderLocation.getPort());
        }
        return null;
    }

    @Override
    protected CoordinatorServiceGrpc.CoordinatorServiceBlockingStub newStub(ManagedChannel channel) {
        return CoordinatorServiceGrpc.newBlockingStub(channel);
    }
}
