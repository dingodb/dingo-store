/*
 * Copyright 2021, Zetyun DataPortal All rights reserved.
 */

package io.dingodb.client;

public final class OperationFactory {

    public static IStoreOperation getStoreOperation(StoreOperationType type) {
        switch (type) {
            case GET:
                return GetOperation.getInstance();
            case PUT:
                return PutOperation.getInstance();
            default:
                return null;
        }
    }
}
