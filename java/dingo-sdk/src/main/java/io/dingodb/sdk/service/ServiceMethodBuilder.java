package io.dingodb.sdk.service;

import io.dingodb.sdk.grpc.serializer.Marshaller;
import io.dingodb.sdk.service.entity.Message;
import io.grpc.MethodDescriptor;

import java.util.function.Supplier;

public class ServiceMethodBuilder {

    public static <REQ extends Message, RES extends Message> MethodDescriptor<REQ, RES> buildUnary(
        String fullName, Supplier<REQ> reqSupplier, Supplier<RES> resSupplier
    ) {
        return MethodDescriptor.<REQ, RES>newBuilder()
            .setType(MethodDescriptor.MethodType.UNARY)
            .setFullMethodName(fullName)
            .setSampledToLocalTracing(true)
            .setRequestMarshaller(new Marshaller<>(reqSupplier))
            .setResponseMarshaller(new Marshaller<>(resSupplier))
            .build();
    }

}
