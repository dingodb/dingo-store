package io.dingodb.sdk.grpc.serializer;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import io.dingodb.sdk.service.entity.Message;
import io.grpc.MethodDescriptor;
import lombok.SneakyThrows;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.util.function.Supplier;

public class Marshaller<T extends Message> implements MethodDescriptor.Marshaller<T> {

    private final Supplier<T> supplier;

    public Marshaller(Supplier<T> supplier) {
        this.supplier = supplier;
    }

    public T newInstance() {
        return supplier.get();
    }

    @SneakyThrows
    @Override
    public InputStream stream(T value) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        CodedOutputStream out = CodedOutputStream.newInstance(baos);
        value.write(out);
        out.flush();
        return new ByteArrayInputStream(baos.toByteArray());
    }

    @SneakyThrows
    @Override
    public T parse(InputStream stream) {
        CodedInputStream inputStream = CodedInputStream.newInstance(stream);
        T message = newInstance();
        message.read(inputStream);
        return message;
    }

}
