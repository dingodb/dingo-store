package io.dingodb.sdk.service.entity;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import io.dingodb.sdk.service.entity.error.Error;
import io.dingodb.sdk.service.entity.store.Context;

public interface Message {

    void read(CodedInputStream input);
    void write(CodedOutputStream outputStream);
    int sizeOf();

    interface Response extends Message {
        default Error getError() {
            throw new UnsupportedOperationException();
        }
    }

    interface StoreRequest extends Message {

        Context getContext();

        void setContext(Context context);

    }
}
