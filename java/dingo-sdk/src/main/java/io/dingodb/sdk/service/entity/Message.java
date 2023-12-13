package io.dingodb.sdk.service.entity;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import io.dingodb.sdk.service.entity.common.RequestInfo;
import io.dingodb.sdk.service.entity.common.ResponseInfo;
import io.dingodb.sdk.service.entity.error.Error;
import io.dingodb.sdk.service.entity.store.Context;

public interface Message {

    void read(CodedInputStream input);
    void write(CodedOutputStream outputStream);
    int sizeOf();

    interface Request extends Message {
        default RequestInfo getRequestInfo() {
            throw new UnsupportedOperationException();
        }

        default void setRequestInfo(RequestInfo requestInfo) {
            throw new UnsupportedOperationException();
        }
    }

    interface Response extends Message {
        default ResponseInfo getResponseInfo() {
            throw new UnsupportedOperationException();
        }

        default void setResponseInfo(ResponseInfo responseInfo) {
            throw new UnsupportedOperationException();
        }


        default Error getError() {
            throw new UnsupportedOperationException();
        }
    }

    interface StoreRequest extends Request {

        default Context getContext() {
            return null;
        }

        default void setContext(Context context) {
        }

    }
}
