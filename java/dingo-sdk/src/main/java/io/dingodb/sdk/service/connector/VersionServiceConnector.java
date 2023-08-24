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
import io.dingodb.sdk.common.DingoClientException;
import io.dingodb.sdk.common.Location;
import io.dingodb.sdk.common.utils.Optional;
import io.dingodb.version.Version;
import io.dingodb.version.VersionServiceGrpc;
import io.grpc.ManagedChannel;
import lombok.Getter;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class VersionServiceConnector extends ServiceConnector<VersionServiceGrpc.VersionServiceBlockingStub> {

    private final ScheduledExecutorService executors = Executors.newScheduledThreadPool(1);

    @Getter
    private final CoordinatorServiceConnector coordinatorServiceConnector;

    public final int leaseTtl;

    @Getter
    private long lease = -1;

    public VersionServiceConnector(String locations, int leaseTtl) {
        super(Collections.emptySet());
        this.coordinatorServiceConnector = new CoordinatorServiceConnector(locations);
        this.leaseTtl = leaseTtl;
        executors.scheduleWithFixedDelay(this::renewLease, leaseTtl, leaseTtl / 3, TimeUnit.SECONDS);
    }

    public VersionServiceConnector(Set<Location> locations, int leaseTtl) {
        super(Collections.emptySet());
        this.coordinatorServiceConnector = new CoordinatorServiceConnector(locations);
        this.leaseTtl = leaseTtl;
        executors.scheduleWithFixedDelay(this::renewLease, leaseTtl, leaseTtl / 3, TimeUnit.SECONDS);
    }

    public void close() {
        executors.shutdown();
    }

    @Override
    protected ManagedChannel transformToLeaderChannel(ManagedChannel channel) {
        return Optional.ofNullable(coordinatorServiceConnector.getCoordinatorMap())
                .map(Coordinator.GetCoordinatorMapResponse::getLeaderLocation)
                .filter(__ -> !__.getHost().isEmpty())
                .map(__ -> newChannel(__.getHost(), __.getPort()))
                .orNull();
    }

    @Override
    protected VersionServiceGrpc.VersionServiceBlockingStub newStub(ManagedChannel channel) {
        VersionServiceGrpc.VersionServiceBlockingStub stub = VersionServiceGrpc.newBlockingStub(channel);
        Version.LeaseGrantResponse response = stub.leaseGrant(
                Version.LeaseGrantRequest.newBuilder().setTTL(leaseTtl).build()
        );
        if (response.getError().getErrcode().getNumber() == 0) {
            this.lease = response.getID();
            return stub;
        }
        throw new DingoClientException(response.getError().getErrcode().getNumber(), response.getError().getErrmsg());
    }

    private void renewLease() {
        if (lease == -1) {
            return;
        }
        exec(stub -> stub.leaseRenew(Version.LeaseRenewRequest.newBuilder().setID(lease).build()));
    }

}
