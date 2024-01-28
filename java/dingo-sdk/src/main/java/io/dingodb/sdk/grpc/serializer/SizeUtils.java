package io.dingodb.sdk.grpc.serializer;

import io.dingodb.sdk.service.entity.Message;
import io.dingodb.sdk.service.entity.Numeric;

import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;

import static com.google.protobuf.CodedOutputStream.computeBoolSize;
import static com.google.protobuf.CodedOutputStream.computeBoolSizeNoTag;
import static com.google.protobuf.CodedOutputStream.computeDoubleSize;
import static com.google.protobuf.CodedOutputStream.computeDoubleSizeNoTag;
import static com.google.protobuf.CodedOutputStream.computeEnumSize;
import static com.google.protobuf.CodedOutputStream.computeEnumSizeNoTag;
import static com.google.protobuf.CodedOutputStream.computeFloatSize;
import static com.google.protobuf.CodedOutputStream.computeFloatSizeNoTag;
import static com.google.protobuf.CodedOutputStream.computeInt32Size;
import static com.google.protobuf.CodedOutputStream.computeInt32SizeNoTag;
import static com.google.protobuf.CodedOutputStream.computeInt64Size;
import static com.google.protobuf.CodedOutputStream.computeInt64SizeNoTag;
import static com.google.protobuf.CodedOutputStream.computeStringSize;
import static com.google.protobuf.CodedOutputStream.computeStringSizeNoTag;
import static com.google.protobuf.CodedOutputStream.computeTagSize;
import static com.google.protobuf.CodedOutputStream.computeUInt32SizeNoTag;

public class SizeUtils {
    public static Integer sizeOf(Boolean value) {
        return value == null ? 0 : computeBoolSizeNoTag(value);
    }

    public static Integer sizeOf(Integer value) {
        return value == null ? 0 : computeInt32SizeNoTag(value);
    }

    public static Integer sizeOf(Long value) {
        return value == null ? 0 : computeInt64SizeNoTag(value);
    }

    public static Integer sizeOf(Float value) {
        return value == null ? 0 : computeFloatSizeNoTag(value);
    }

    public static Integer sizeOf(Double value) {
        return value == null ? 0 : computeDoubleSizeNoTag(value);
    }

    public static Integer sizeOf(Numeric value) {
        return value == null ? 0 : computeEnumSizeNoTag(value.number());
    }


    public static Integer sizeOf(String value) {
        return value == null ? 0 : computeStringSizeNoTag(value);
    }

    public static Integer sizeOf(byte[] value) {
        return value == null ? 0 : computeUInt32SizeNoTag(value.length) + value.length;
    }

    public static Integer sizeOf(Message value) {
        if (value != null) {
            int size = value.sizeOf();
            size += computeInt32SizeNoTag(size);
            return size;
        }
        return 0;
    }

    public static Integer sizeOf(Integer number, Boolean value) {
        return value == null ? 0 : computeBoolSize(number, value);
    }

    public static Integer sizeOf(Integer number, Integer value) {
        return value == null ? 0 : computeInt32Size(number, value);
    }

    public static Integer sizeOf(Integer number, Long value) {
        return value == null ? 0 : computeInt64Size(number, value);
    }

    public static Integer sizeOf(Integer number, Float value) {
        return value == null ? 0 : computeFloatSize(number, value);
    }

    public static Integer sizeOf(Integer number, Double value) {
        return value == null ? 0 : computeDoubleSize(number, value);
    }

    public static Integer sizeOf(Integer number, Numeric value) {
        return value == null ? 0 : computeEnumSize(number, value.number());
    }

    public static Integer sizeOf(Integer number, String value) {
        return value == null ? 0 : computeStringSize(number, value);
    }

    public static Integer sizeOf(Integer number, byte[] value) {
        return value == null ? 0 : computeTagSize(number) + sizeOf(value);
    }

    public static Integer sizeOf(Integer number, Message value) {
        if (value != null) {
            int size = value.sizeOf();
            size += computeInt32SizeNoTag(size);
            size += computeTagSize(number);
            return size;
        }
        return 0;
    }

    public static Integer sizeOf(Numeric numeric, Message value) {
        if (value != null) {
            int size = value.sizeOf();
            if (!value.isDirect()) {
                size += computeInt32SizeNoTag(size);
                size += computeTagSize(numeric.number());
            }
            return size;
        }
        return 0;
    }

    public static <T> int sizeOfPack(Integer number, List<T> value, Function<T, Integer> sizeComputer) {
        if (value != null && value.size() > 0) {
            int size = 0;
            for (T element : value) {
                size += sizeComputer.apply(element);
            }
            size = computeUInt32SizeNoTag(size) + size + computeTagSize(number);
            return size;
        }
        return 0;
    }

    public static <T> int sizeOf(Integer number, List<T> value, Function<T, Integer> sizeComputer) {
        if (value != null && value.size() > 0) {
            int size = 0;
            for (T element : value) {
                size += computeTagSize(number);
                size += sizeComputer.apply(element);
            }
            return size;
        }
        return 0;
    }

    public static <K, V> int sizeOf(
        Integer number,
        Map<K, V> value,
        BiFunction<Integer, K, Integer> keySizeComputer,
        BiFunction<Integer, V, Integer> valueSizeComputer
    ) {
        if (value != null && value.size() > 0) {
            int size = 0;
            for (Map.Entry<K, V> entry : value.entrySet()) {
                int entrySize = 0;
                size += computeTagSize(number);
                entrySize += keySizeComputer.apply(1, entry.getKey());
                entrySize += valueSizeComputer.apply(2, entry.getValue());
                entrySize += computeUInt32SizeNoTag(entrySize);
                size += entrySize;
            }
            return size;
        }
        return 0;
    }
}
