package io.dingodb.grpc;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.ParameterSpec;
import io.grpc.stub.annotations.RpcMethod;


public class Constant {
    public static final String SERVICE_PKG = "io.dingodb.sdk.service";
    public static final String SERVICE_DESC_PKG = "io.dingodb.sdk.service.desc";
    public static final String MSG_PKG = "io.dingodb.sdk.service.entity";

    public static final String GRPC_ANNOTATION = RpcMethod.class.getName();


    public static final String FIELD_NUMBER_END = "_FIELD_NUMBER";
    public static final String FIELD_END = "_";
    public static final String CASE_END = "Case_";
    public static final String NUMBER = "number";
    public static final String VALUE = "value";
    public static final String OUT = "out";
    public static final String INPUT = "input";

    public static final ParameterSpec OUT_ARG = ParameterSpec.builder(CodedOutputStream.class, OUT).build();
    public static final ParameterSpec INPUT_ARG = ParameterSpec.builder(CodedInputStream.class, INPUT).build();



    public static final ClassName SERVICE = ClassName.get(SERVICE_PKG, "Service");
    public static final ClassName SERVICE_METHOD_BUILDER = ClassName.get(SERVICE_PKG, "ServiceMethodBuilder");
    public static final ClassName SERVICE_CALL_CYCLE = ClassName.get(SERVICE_PKG, "ServiceCallCycles");
    public static final ClassName CALLER = ClassName.get(SERVICE_PKG, "Caller");
    public static final ClassName FUTURE = ClassName.get(SERVICE_PKG, "RpcFuture");
    public static final ClassName NUMERIC = ClassName.get(MSG_PKG, "Numeric");
    public static final ClassName MESSAGE = ClassName.get(MSG_PKG, "Message");
    public static final ClassName FIELD = ClassName.get(MSG_PKG, "Field");
    public static final ClassName COMMON_ID = ClassName.get(MSG_PKG + ".meta", "DingoCommonId");
    public static final ClassName REQUEST = MESSAGE.nestedClass("Request");
    public static final ClassName RESPONSE = MESSAGE.nestedClass("Response");
    public static final ClassName STORE_REQ = MESSAGE.nestedClass("StoreRequest");

    public static final String SERIALIZE_PACKAGE = "io.dingodb.sdk.grpc.serializer";

    public static final String HAS_VALUE = "hasValue";
    public static final String WRITE = "write";
    public static final String SIZE_OF = "sizeOf";

    public static final ClassName WRITER = ClassName.get(SERIALIZE_PACKAGE, "Writer");
    public static final ClassName READER = ClassName.get(SERIALIZE_PACKAGE, "Reader");
    public static final ClassName SIZE_UTILS = ClassName.get(SERIALIZE_PACKAGE, "SizeUtils");

    public static final ClassName MARSHALLER = ClassName.get(SERIALIZE_PACKAGE, "Marshaller");

}
