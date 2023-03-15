/*
 * Copyright 2021, Zetyun DataPortal All rights reserved.
 */

package io.dingodb.client;

import io.dingodb.sdk.service.store.StoreServiceClient;

public interface IStoreOperation {

    ResultForStore doOperation(StoreServiceClient storeServiceClient, ContextForStore contextForStore);
}
