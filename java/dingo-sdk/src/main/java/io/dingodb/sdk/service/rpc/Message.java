package io.dingodb.sdk.service.rpc;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import io.dingodb.sdk.service.rpc.message.error.Error;
import io.dingodb.sdk.service.rpc.message.store.Context;

public interface Message {

    void read(CodedInputStream input);
    void write(CodedOutputStream outputStream);
    int sizeOf();

    interface Response extends Message {
        default Error error() {
            throw new UnsupportedOperationException();
        }
    }

    interface StoreRequest extends Message {

        Context context();

        StoreRequest context(Context context);

    }
}
