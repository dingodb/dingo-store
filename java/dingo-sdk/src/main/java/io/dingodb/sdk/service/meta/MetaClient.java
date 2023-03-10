/*
 * Copyright 2021, Zetyun DataPortal All rights reserved.
 */

package io.dingodb.sdk.service.meta;

import io.dingodb.sdk.service.connector.ServiceConnector;
import lombok.experimental.Delegate;

public class MetaClient {
    @Delegate
    private MetaServiceClient metaServiceClient;
    private String target;

    public MetaClient(String target) {
        this.target = target;
    }

    public void init() {
        ServiceConnector serviceConnector = new ServiceConnector(target);
        this.metaServiceClient = new MetaServiceClient(serviceConnector);
        // TODO retry
    }
}
