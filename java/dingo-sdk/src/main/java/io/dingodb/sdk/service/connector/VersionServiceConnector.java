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
import io.dingodb.error.ErrorOuterClass;
import io.dingodb.sdk.common.DingoClientException;
import io.dingodb.sdk.common.Location;
import io.dingodb.sdk.common.utils.ErrorCodeUtils;
import io.dingodb.sdk.common.utils.Optional;
import io.dingodb.sdk.common.utils.Parameters;
import io.dingodb.version.Version;
import io.dingodb.version.VersionServiceGrpc;
import io.grpc.ManagedChannel;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import static io.dingodb.sdk.common.utils.ErrorCodeUtils.Strategy.RETRY;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class VersionServiceConnector extends ServiceConnector<VersionServiceGrpc.VersionServiceBlockingStub> {

    private final ScheduledExecutorService executors = Executors.newScheduledThreadPool(1);

    private final CoordinatorServiceConnector coordinatorServiceConnector;

    public final int leaseTtl;

    private final AtomicLong lease = new AtomicLong(-1);

    public VersionServiceConnector(String locations, int leaseTtl) {
        super(Collections.emptySet());
        Parameters.check(leaseTtl, ttl -> ttl >= 1, "Lease ttl must great than 1.");
        this.coordinatorServiceConnector = new CoordinatorServiceConnector(locations);
        this.leaseTtl = leaseTtl;
        initRenew();
    }

    public VersionServiceConnector(Set<Location> locations, int leaseTtl) {
        super(Collections.emptySet());
        Parameters.check(leaseTtl, ttl -> ttl >= 1, "Lease ttl must great than 1.");
        this.coordinatorServiceConnector = new CoordinatorServiceConnector(locations);
        this.leaseTtl = leaseTtl;
        initRenew();
    }

    private void initRenew() {
        int delay = Math.max(Math.abs(leaseTtl * 1000) / 3, 1000);
        executors.scheduleWithFixedDelay(this::renewLease, leaseTtl, delay, MILLISECONDS);
    }

    public void close() {
        super.close();
        executors.shutdown();
    }

    public long getLease() {
        return this.lease.get();
    }

    @Override
    public <R> Response<R> exec(
        Function<VersionServiceGrpc.VersionServiceBlockingStub, R> function,
        int retryTimes,
        Function<Integer, ErrorCodeUtils.Strategy> errChecker,
        Function<R, Response<R>> toResponse
    ) {
        return super.exec(function, leaseTtl, __ -> RETRY, toResponse);
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

        if (
            stub.leaseQuery(
                Version.LeaseQueryRequest.newBuilder().setID(lease.get()).build()
            ).getError().getErrcode() == ErrorOuterClass.Errno.OK
        ) {
            return stub;
        }

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
        exec(
            "renew-lease",
            stub -> stub.leaseRenew(Version.LeaseRenewRequest.newBuilder().setID(getLease()).build()),
            Integer.MAX_VALUE,
            __ -> RETRY,
            this::toResponse
        );
    }

    public CoordinatorServiceConnector getCoordinatorServiceConnector() {
        return this.coordinatorServiceConnector;
    }

}
