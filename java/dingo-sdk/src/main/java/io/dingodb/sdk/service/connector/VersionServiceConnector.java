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
import io.dingodb.sdk.common.utils.ErrorCodeUtils;
import io.dingodb.sdk.common.utils.Optional;
import io.dingodb.version.Version;
import io.dingodb.version.VersionServiceGrpc;
import io.grpc.ManagedChannel;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

public class VersionServiceConnector extends ServiceConnector<VersionServiceGrpc.VersionServiceBlockingStub> {

    private final ScheduledExecutorService executors = Executors.newScheduledThreadPool(1);

    private final CoordinatorServiceConnector coordinatorServiceConnector;

    public final int leaseTtl;

    private final AtomicLong lease = new AtomicLong(-1);

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

    public long getLease() {
        return this.lease.get();
    }

    @Override
    public <R> Response<R> exec(
        Function<VersionServiceGrpc.VersionServiceBlockingStub, R> function,
        int retryTimes,
        Function<Integer, ErrorCodeUtils.InternalCode> errChecker,
        Function<R, Response<R>> toResponse
    ) {
        return super.exec(function, leaseTtl, errChecker, toResponse);
    }

    @Override
    protected ManagedChannel transformToLeaderChannel(ManagedChannel channel) {
        return Optional.ofNullable(coordinatorServiceConnector.getCoordinatorMap())
            .map(Coordinator.GetCoordinatorMapResponse::getKvLeaderLocation)
            .filter(__ -> !__.getHost().isEmpty())
            .map(__ -> newChannel(__.getHost(), __.getPort()))
            .orNull();
    }

    @Override
    protected VersionServiceGrpc.VersionServiceBlockingStub newStub(ManagedChannel channel) {
        VersionServiceGrpc.VersionServiceBlockingStub stub = VersionServiceGrpc.newBlockingStub(channel);
        Version.LeaseGrantResponse response = stub.leaseGrant(
            Version.LeaseGrantRequest.newBuilder().setID(System.currentTimeMillis()).setTTL(leaseTtl).build()
        );
        if (response.getError().getErrcode().getNumber() == 0) {
            lease.compareAndSet(-1, response.getID());
            return stub;
        }
        throw new DingoClientException(response.getError().getErrcode().getNumber(), response.getError().getErrmsg());
    }

    private void renewLease() {
        if (getLease() == -1) {
            return;
        }
        exec(stub -> stub.leaseRenew(Version.LeaseRenewRequest.newBuilder().setID(getLease()).build()));
    }

    public CoordinatorServiceConnector getCoordinatorServiceConnector() {
        return this.coordinatorServiceConnector;
    }

}
