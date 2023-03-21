/*
 * Copyright 2021, Zetyun DataPortal All rights reserved.
 */

package io.dingodb.sdk.service.connector;

import io.dingodb.common.Common;
import io.dingodb.coordinator.Coordinator;
import io.dingodb.coordinator.CoordinatorServiceGrpc;
import io.dingodb.sdk.common.utils.GrpcConnection;
import io.dingodb.meta.MetaServiceGrpc;
import io.grpc.ManagedChannel;
import lombok.Getter;

import java.util.concurrent.TimeUnit;

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
            target = leaderLocation.getHost() + ":" + leaderLocation.getPort();
            channel.shutdownNow();
            channel = GrpcConnection.newChannel(target);
            metaBlockingStub = MetaServiceGrpc.newBlockingStub(channel);
        }
    }

    public void shutdown() {
        try {
            channel.shutdown().awaitTermination(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
