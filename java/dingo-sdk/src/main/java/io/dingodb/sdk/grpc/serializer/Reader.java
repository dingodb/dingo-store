package io.dingodb.sdk.grpc.serializer;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.WireFormat;
import io.dingodb.sdk.common.utils.Parameters;
import io.dingodb.sdk.service.entity.Message;
import lombok.SneakyThrows;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public class Reader {

    @SneakyThrows
    public static int readNumber(CodedInputStream input) {
        return WireFormat.getTagFieldNumber(input.readTag());
    }

    @SneakyThrows
    public static boolean readBoolean(CodedInputStream input) {
        return input.readBool();
    }

    @SneakyThrows
    public static int readInt(CodedInputStream input) {
        return input.readInt32();
    }

    @SneakyThrows
    public static long readLong(CodedInputStream input) {
        return input.readInt64();
    }

    @SneakyThrows
    public static float readFloat(CodedInputStream input) {
        return input.readFloat();
    }

    @SneakyThrows
    public static double readDouble(CodedInputStream input) {
        return input.readDouble();
    }

    @SneakyThrows
    public static byte[] readBytes(CodedInputStream input) {
        return input.readByteArray();
    }

    @SneakyThrows
    public static String readString(CodedInputStream input) {
        return input.readString();
    }

    @SneakyThrows
    public static void skip(CodedInputStream input) {
        input.skipField(input.getLastTag());
    }

    @SneakyThrows
    public static <M extends Message> M readMessage(M message, CodedInputStream input) {
        if (message.isDirect()) {
            message.read(input);
            return message;
        }
        final int length = input.readRawVarint32();
        input.checkRecursionLimit();
        int oldLimit = input.pushLimit(length);
        boolean hasValue = message.read(input);
        input.checkLastTagWas(0);
        if (input.getBytesUntilLimit() != 0) {
            throw new RuntimeException("The message " + message.getClass() + " not read finish.");
        }
        input.popLimit(oldLimit);
        return hasValue ? message : null;
    }

    @SneakyThrows
    public static <M extends Message> M readMessage(CodedInputStream input, Function<CodedInputStream, M> reader) {
        int length = input.readRawVarint32();
        int oldLimit = input.pushLimit(length);
        M result = reader.apply(input);
        input.popLimit(oldLimit);
        return result;
    }

    @SneakyThrows
    public static <T> List<T> readPack(CodedInputStream input, Function<CodedInputStream, T> reader) {
        List<T> result = new ArrayList<>();
        int length = input.readRawVarint32();
        int oldLimit = input.pushLimit(length);
        while(input.getBytesUntilLimit() > 0) {
            result.add(reader.apply(input));
        }
        input.popLimit(oldLimit);
        return Collections.unmodifiableList(result);
    }

    @SneakyThrows
    public static <T> List<T> readList(List<T> current, CodedInputStream input, Function<CodedInputStream, T> reader) {
        current = Parameters.cleanNull(current, ArrayList::new);
        current.add(reader.apply(input));
        return current;
    }

    @SneakyThrows
    public static <K, V> Map<K, V> readMap(
        int number,
        Map<K, V> current,
        CodedInputStream input,
        Function<CodedInputStream, K> keyReader,
        Function<CodedInputStream, V> valueReader
    ) {
        current = Parameters.cleanNull(current, HashMap::new);
        Map.Entry<K, V> entry = readMap(input, keyReader, valueReader);
        current.put(entry.getKey(), entry.getValue());
        return current;
    }

    @SneakyThrows
    public static <K, V> Map.Entry<K, V> readMap(
        CodedInputStream input, Function<CodedInputStream, K> keyReader, Function<CodedInputStream, V> valueReader
    ) {
        int length = input.readRawVarint32();
        int oldLimit = input.pushLimit(length);

        K key = null;
        V value = null;

        int number;
        while((number = readNumber(input)) != 0) {
            if (number == 1) {
                key = keyReader.apply(input);
            }
            if (number == 2) {
                value = valueReader.apply(input);
            }
        }
        input.popLimit(oldLimit);
        return new AbstractMap.SimpleEntry<>(key, value);
    }

}
