package io.dingodb.sdk.service.util;

import io.dingodb.sdk.service.entity.Message;
import io.dingodb.sdk.service.entity.store.TxnBatchGetRequest;
import io.dingodb.sdk.service.entity.store.TxnPrewriteRequest;
import io.dingodb.sdk.service.entity.store.TxnCommitRequest;
import io.dingodb.sdk.service.entity.store.TxnScanRequest;
import io.dingodb.sdk.service.entity.store.TxnGetRequest;
import io.dingodb.sdk.service.entity.store.TxnBatchRollbackRequest;

public final class OnlineDdlLogUtils {
    public static long regionId = 0L;
    public static boolean enableDdlSqlLog = false;

    public static boolean onlineDdlReq(Message.Request req) {
        if (enableDdlSqlLog) {
            return true;
        }
        if(req instanceof TxnPrewriteRequest) {
            return ((TxnPrewriteRequest) req).getContext().getRegionId() > regionId;
        } else if (req instanceof TxnBatchGetRequest) {
             return ((TxnBatchGetRequest) req).getContext().getRegionId() > regionId;
        } else if (req instanceof TxnCommitRequest) {
            return ((TxnCommitRequest) req).getContext().getRegionId() > regionId;
        } else if (req instanceof TxnScanRequest) {
            return ((TxnScanRequest) req).getContext().getRegionId() > regionId;
        } else if (req instanceof TxnGetRequest) {
            return ((TxnGetRequest) req).getContext().getRegionId() > regionId;
        } else if (req instanceof TxnBatchRollbackRequest) {
            return ((TxnBatchRollbackRequest) req).getContext().getRegionId() > regionId;
        }
        return true;
    }
}
