/*
 * Copyright 2021, Zetyun DataPortal All rights reserved.
 */

package io.dingodb.sdk.service.connector;

import io.dingodb.sdk.common.utils.GrpcConnection;
import io.dingodb.meta.MetaServiceGrpc;
import io.grpc.ManagedChannel;
import lombok.Getter;

public class ServiceConnector {

    private String target;
    @Getter
    private MetaServiceGrpc.MetaServiceBlockingStub metaBlockingStub;

    public ServiceConnector(String target) {
        this.target = target;
    }

    public void initConnection() {
        // TODO connection coordinator leader

        ManagedChannel channel = GrpcConnection.newChannel(target);
        metaBlockingStub = MetaServiceGrpc.newBlockingStub(channel);
    }
}
