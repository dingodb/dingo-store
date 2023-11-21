package io.dingodb.sdk.service.rpc;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import io.dingodb.sdk.grpc.serializer.SizeUtils;
import io.dingodb.sdk.grpc.serializer.Writer;
import lombok.SneakyThrows;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public abstract class Field<T> {

    protected T value;

    public abstract Integer number();

    public void read(CodedInputStream input) {

    }

    @SneakyThrows
    public void write(CodedOutputStream out) {
        if (value != null) {
            if (value instanceof Enum) {
                Writer.write(number(), (Enum) value(), out);
            } else {
                Writer.write(number(), (Message) value(), out);
            }
        }
    }

    public Integer sizeOf() {
        if (value != null) {
            if (value instanceof Enum) {
                return SizeUtils.sizeOf(number(), (Enum) value);
            }
            return SizeUtils.sizeOf(number(), (Message) value);
        }
        return 0;
    }

    public T value() {
        if (value instanceof List) {
            return (T) Collections.unmodifiableList((List) value);
        }
        if (value instanceof Map) {
            return (T) Collections.unmodifiableMap((Map) value);
        }
        return value;
    }

    public Field<T> value(T value) {
        this.value = value;
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Field<?> field = (Field<?>) o;
        return Objects.equals(number(), field.number()) && Objects.equals(value, field.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(number(), value);
    }
}
