package io.dingodb.sdk.grpc.serializer;

import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.WireFormat;
import io.dingodb.sdk.service.entity.Message;
import io.dingodb.sdk.service.entity.Numeric;
import lombok.SneakyThrows;

import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Function;

import static com.google.protobuf.CodedOutputStream.computeTagSize;
import static com.google.protobuf.WireFormat.FieldType.BOOL;
import static com.google.protobuf.WireFormat.FieldType.DOUBLE;
import static com.google.protobuf.WireFormat.FieldType.FLOAT;
import static com.google.protobuf.WireFormat.FieldType.INT32;
import static com.google.protobuf.WireFormat.FieldType.INT64;

public class Writer {

    @SneakyThrows
    public static void write(Boolean value, CodedOutputStream out) {
        if (value != null) {
            out.writeBoolNoTag(value);
        }
    }

    @SneakyThrows
    public static void write(Integer value, CodedOutputStream out) {
        if (value != null) {
            out.writeInt32NoTag(value);
        }
    }

    @SneakyThrows
    public static void write(Long value, CodedOutputStream out) {
        if (value != null) {
            out.writeInt64NoTag(value);
        }
    }

    @SneakyThrows
    public static void write(Float value, CodedOutputStream out) {
        if (value != null) {
            out.writeFloatNoTag(value);
        }
    }

    @SneakyThrows
    public static void write(Double value, CodedOutputStream out) {
        if (value != null) {
            out.writeDoubleNoTag(value);
        }
    }

    @SneakyThrows
    public static void write(Numeric value, CodedOutputStream out) {
        if (value != null) {
            write(value.number(), out);
        }
    }

    @SneakyThrows
    public static void write(String value, CodedOutputStream out) {
        if (value != null) {
            out.writeStringNoTag(value);
        }
    }

    @SneakyThrows
    public static void write(byte[] value, CodedOutputStream out) {
        if (value != null) {
            out.writeUInt32NoTag(value.length);
            out.write(value, 0, value.length);
        }
    }

    @SneakyThrows
    public static void write(Message value, CodedOutputStream out) {
        if (value != null) {
            out.writeUInt32NoTag(value.sizeOf());
            value.write(out);
        }
    }

    @SneakyThrows
    public static void write(Integer number, Boolean value, CodedOutputStream out) {
        if (value != null) {
            out.writeTag(number, BOOL.getWireType());
            write(value, out);
        }
    }

    @SneakyThrows
    public static void write(Integer number, Integer value, CodedOutputStream out) {
        if (value != null) {
            out.writeTag(number, INT32.getWireType());
            write(value, out);
        }
    }

    @SneakyThrows
    public static void write(Integer number, Long value, CodedOutputStream out) {
        if (value != null) {
            out.writeTag(number, INT64.getWireType());
            write(value, out);
        }
    }

    @SneakyThrows
    public static void write(Integer number, Float value, CodedOutputStream out) {
        if (value != null) {
            out.writeTag(number, FLOAT.getWireType());
            write(value, out);
        }
    }

    @SneakyThrows
    public static void write(Integer number, Double value, CodedOutputStream out) {
        if (value != null) {
            out.writeTag(number, DOUBLE.getWireType());
            write(value, out);
        }
    }

    @SneakyThrows
    public static void write(Integer number, Numeric value, CodedOutputStream out) {
        if (value != null) {
            write(number, value.number(), out);
        }
    }

    @SneakyThrows
    public static void write(Integer number, String value, CodedOutputStream out) {
        if (value != null) {
            out.writeTag(number, WireFormat.WIRETYPE_LENGTH_DELIMITED);
            out.writeStringNoTag(value);
        }
    }

    @SneakyThrows
    public static void write(Integer number, byte[] value, CodedOutputStream out) {
        if (value != null) {
            out.writeTag(number, WireFormat.WIRETYPE_LENGTH_DELIMITED);
            out.writeByteArrayNoTag(value);
        }
    }

    @SneakyThrows
    public static void write(Integer number, Message value, CodedOutputStream out) {
        if (value != null) {
            if (!value.isDirect()) {
                out.writeTag(number, WireFormat.WIRETYPE_LENGTH_DELIMITED);
                out.writeUInt32NoTag(value.sizeOf());
            }
            value.write(out);
        }
    }

    @SneakyThrows
    public static void write(Numeric numeric, Message value, CodedOutputStream out) {
        if (value != null) {
            if (!value.isDirect()) {
                out.writeTag(numeric.number(), WireFormat.WIRETYPE_LENGTH_DELIMITED);
                out.writeUInt32NoTag(value.sizeOf());
            }
            value.write(out);
        }
    }

    @SneakyThrows
    public static <M> void write(
        Integer number, List<M> value, BiConsumer<Integer, M> serializer
    ) {
        if (value != null) {
            value.forEach(v -> serializer.accept(number, v));
        }
    }

    @SneakyThrows
    public static <T> void write(
        Integer number,
        List<T> value,
        CodedOutputStream out,
        BiConsumer<T, CodedOutputStream> serializer,
        Function<T, Integer> sizeComputer
    ) {
        if (value != null && value.size() > 0) {
            out.writeTag(number, WireFormat.WIRETYPE_LENGTH_DELIMITED);
            Integer size = 0;
            for (T element : value) {
                size += sizeComputer.apply(element);
            }
            out.writeUInt32NoTag(size);
            value.forEach(element -> serializer.accept(element, out));
        }
    }

    @SneakyThrows
    public static <K, V> void write(
        Integer number,
        Map<K, V> value,
        CodedOutputStream out,
        BiConsumer<Integer, K> keySerializer,
        BiConsumer<Integer, V> valueSerializer,
        Function<K, Integer> keySizeComputer,
        Function<V, Integer> valueSizeComputer
    ) {
        if (value != null && value.size() > 0) {
            for (Map.Entry<K, V> entry : value.entrySet()) {
                out.writeTag(number, WireFormat.WIRETYPE_LENGTH_DELIMITED);
                K k = entry.getKey();
                V v = entry.getValue();
                out.writeUInt32NoTag(
                    computeTagSize(1) + keySizeComputer.apply(k) + computeTagSize(2) + valueSizeComputer.apply(v)
                );
                keySerializer.accept(1, k);
                valueSerializer.accept(2, v);
            }
        }
    }

}
