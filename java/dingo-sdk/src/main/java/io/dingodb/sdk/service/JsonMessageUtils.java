package io.dingodb.sdk.service;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.base.CaseFormat;
import io.dingodb.sdk.common.utils.ByteArrayUtils;
import io.dingodb.sdk.common.utils.StackTraces;
import io.dingodb.sdk.service.entity.Message;
import io.dingodb.sdk.service.entity.ServiceCallCycleEntity;
import io.grpc.CallOptions;
import lombok.SneakyThrows;

import java.io.IOException;

import static com.google.common.base.CaseFormat.LOWER_CAMEL;
import static com.google.common.base.CaseFormat.UPPER_CAMEL;
import static io.dingodb.sdk.common.utils.StackTraces.CURRENT_STACK;

public class JsonMessageUtils {

    private static final ObjectMapper jsonMapper = new ObjectMapper();

    static {
        SimpleModule simpleModule = new SimpleModule();
        ByteArraySerializer bytesSerializer = new ByteArraySerializer();
        simpleModule.addSerializer(byte[].class, bytesSerializer);
        jsonMapper.registerModule(simpleModule);
    }

    private static class ByteArraySerializer extends JsonSerializer<byte[]> {
        @Override
        public void serialize(byte[] value, JsonGenerator gen, SerializerProvider serializers) throws IOException {
            gen.writeStartObject();
            gen.writeFieldName("bytes");
            gen.writeStartArray();
            for (byte b : value) {
                gen.writeNumber(b);
            }
            gen.writeEndArray();
            gen.writeFieldName("hex");
            gen.writeString(ByteArrayUtils.toHex(value));
            gen.writeEndObject();
        }
    }

    @SneakyThrows
    public static String toJson(Message message) {
        return jsonMapper.writeValueAsString(message);
    }

    @SneakyThrows
    public static String toJson(ServiceCallCycleEntity entity) {
        return jsonMapper.writeValueAsString(entity);
    }

    @SneakyThrows
    public static String toJson(
        String method,
        long trace,
        Message.Request request,
        Message.Response response,
        CallOptions options
    ) {
        return jsonMapper.writeValueAsString(
            ServiceCallCycleEntity.builder()
                .trace(trace)
                .options(options)
                .method(method)
                .step(LOWER_CAMEL.to(UPPER_CAMEL, StackTraces.methodName(CURRENT_STACK + 1)))
                .request(request)
                .response(response)
                .build()
        );
    }

    @SneakyThrows
    public static String toJson(
        String remote,
        String method,
        long trace,
        Message.Request request,
        Message.Response response,
        CallOptions options
    ) {
        return jsonMapper.writeValueAsString(
            ServiceCallCycleEntity.builder()
                .trace(trace)
                .options(options)
                .method(method)
                .step(LOWER_CAMEL.to(UPPER_CAMEL, StackTraces.methodName(CURRENT_STACK + 1)))
                .remote(remote)
                .request(request)
                .response(response)
                .build()
        );
    }

    @SneakyThrows
    public static String toJson(
        String status,
        String remote,
        String method,
        long trace,
        Message.Request request,
        Message.Response response,
        CallOptions options,
        Exception exception
    ) {
        return jsonMapper.writeValueAsString(
            ServiceCallCycleEntity.builder()
                .trace(trace)
                .options(options)
                .method(method)
                .step(LOWER_CAMEL.to(UPPER_CAMEL, StackTraces.methodName(CURRENT_STACK + 1)))
                .remote(remote)
                .request(request)
                .response(response)
                .exception(exception)
                .status(status)
                .build()
        );
    }

}
