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
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Slf4j
public class MetaServiceConnector extends ServiceConnector<MetaServiceGrpc.MetaServiceBlockingStub> {

    @Getter
    private final CoordinatorServiceConnector coordinatorServiceConnector;

    @Deprecated
    public MetaServiceConnector(List<Location> locations) {
        this(new HashSet<>(locations));
    }

    public MetaServiceConnector(String locations) {
        super(Collections.emptySet());
        this.coordinatorServiceConnector = new CoordinatorServiceConnector(locations);
    }

    public MetaServiceConnector(Set<Location> locations) {
        super(Collections.emptySet());
        this.coordinatorServiceConnector = new CoordinatorServiceConnector(locations);
    }

    public static MetaServiceConnector getMetaServiceConnector(String servers) {
        return new MetaServiceConnector(servers);
    }

    @Override
    public ManagedChannel transformToLeaderChannel(ManagedChannel channel) {
        return Optional.ofNullable(coordinatorServiceConnector.getCoordinatorMap())
            .map(Coordinator.GetCoordinatorMapResponse::getLeaderLocation)
            .filter(__ -> !__.getHost().isEmpty())
            .map(__ -> newChannel(__.getHost(), __.getPort()))
            .orNull();
    }

    @Override
    public MetaServiceGrpc.MetaServiceBlockingStub newStub(ManagedChannel channel) {
        return MetaServiceGrpc.newBlockingStub(channel);
    }

}
