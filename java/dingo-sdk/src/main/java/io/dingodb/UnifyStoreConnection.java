/*
 * Copyright 2021, Zetyun DataPortal All rights reserved.
 */

package io.dingodb;

import io.dingodb.sdk.service.meta.MetaClient;

public class UnifyStoreConnection {

    private MetaClient metaClient;
    private Integer retryTimes;

    public UnifyStoreConnection(String coordinatorSvr, Integer retryTimes) {
        this.metaClient = new MetaClient(coordinatorSvr);
        this.retryTimes = retryTimes;
    }

    public void initConnection() {
        this.metaClient.init();
    }

    public MetaClient getMetaClient() {
        return metaClient;
    }
}
