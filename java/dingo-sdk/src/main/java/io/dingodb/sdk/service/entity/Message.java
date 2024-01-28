package io.dingodb.sdk.service.entity;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import io.dingodb.sdk.common.utils.Optional;
import io.dingodb.sdk.service.entity.common.RequestInfo;
import io.dingodb.sdk.service.entity.common.ResponseInfo;
import io.dingodb.sdk.service.entity.error.Errno;
import io.dingodb.sdk.service.entity.error.Error;
import io.dingodb.sdk.service.entity.store.Context;
import io.dingodb.sdk.service.entity.store.IsolationLevel;

import java.util.List;

public interface Message {

    boolean read(CodedInputStream input);
    void write(CodedOutputStream outputStream);
    int sizeOf();

    @JsonIgnore
    default boolean isDirect() {
        return false;
    }

    @JsonIgnore
    default Object getExt$() {
        throw new UnsupportedOperationException();
    }

    default void setExt$(Object ext$) {
        throw new UnsupportedOperationException();
    }

    interface Request extends Message {
        default RequestInfo getRequestInfo() {
            throw new UnsupportedOperationException();
        }

        default void setRequestInfo(RequestInfo requestInfo) {
            throw new UnsupportedOperationException();
        }
    }

    interface Response extends Message {
        default boolean isOk$() {
            return getError() == null || getError().getErrcode() == null || getError().getErrcode() == Errno.OK;
        }

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

        default void setIsolationLevel(IsolationLevel isolationLevel) {
            if (getContext() == null) {
                setContext(Context.builder().isolationLevel(isolationLevel).build());
            }
            getContext().setIsolationLevel(isolationLevel);
        }

        default void setResolveLocks(List<Long> resolvedLocks) {
            if (getContext() == null) {
                setContext(Context.builder().resolvedLocks(resolvedLocks).build());
            }
            getContext().setResolvedLocks(resolvedLocks);
        }

        default IsolationLevel getIsolationLevel() {
            return Optional.mapOrNull(getContext(), Context::getIsolationLevel);
        }

        default Context getContext() {
            return null;
        }

        default void setContext(Context context) {
        }

    }
}
