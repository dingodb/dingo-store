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

import io.dingodb.coordinator.Coordinator;
import io.dingodb.meta.MetaServiceGrpc;
import io.dingodb.sdk.common.Location;
import io.dingodb.sdk.common.utils.Optional;
import io.grpc.ManagedChannel;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class AutoIncrementServiceConnector extends ServiceConnector<MetaServiceGrpc.MetaServiceBlockingStub> {

    private final CoordinatorServiceConnector coordinatorServiceConnector;

    @Deprecated
    public AutoIncrementServiceConnector(List<Location> locations) {
        this(new HashSet<>(locations));
    }

    public AutoIncrementServiceConnector(String locations) {
        super(Collections.emptySet());
        this.coordinatorServiceConnector = new CoordinatorServiceConnector(locations);
    }

    public AutoIncrementServiceConnector(Set<Location> locations) {
        super(Collections.emptySet());
        this.coordinatorServiceConnector = new CoordinatorServiceConnector(locations);
    }

    public static AutoIncrementServiceConnector getAutoIncrementServiceConnector(String servers) {
        return new AutoIncrementServiceConnector(servers);
    }

    @Override
    protected ManagedChannel transformToLeaderChannel(ManagedChannel channel) {
        return Optional.ofNullable(coordinatorServiceConnector.getCoordinatorMap())
            .map(Coordinator.GetCoordinatorMapResponse::getAutoIncrementLeaderLocation)
            .filter(__ -> !__.getHost().isEmpty())
            .map(__ -> newChannel(__.getHost(), __.getPort()))
            .orNull();
    }

    @Override
    protected MetaServiceGrpc.MetaServiceBlockingStub newStub(ManagedChannel channel) {
        return MetaServiceGrpc.newBlockingStub(channel);
    }
}
