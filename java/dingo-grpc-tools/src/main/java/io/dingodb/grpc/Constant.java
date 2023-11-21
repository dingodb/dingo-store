package io.dingodb.grpc;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.ParameterSpec;
import io.grpc.stub.annotations.RpcMethod;


public class Constant {
    public static final String RPC_PACKAGE = "io.dingodb.sdk.service.rpc";
    public static final String MESSAGE_PACKAGE = "io.dingodb.sdk.service.rpc.message";

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



    public static final ClassName SERVICE = ClassName.get(RPC_PACKAGE, "Service");
    public static final ClassName CALLER = ClassName.get(RPC_PACKAGE, "RpcCaller");
    public static final ClassName FUTURE = ClassName.get(RPC_PACKAGE, "RpcFuture");
    public static final ClassName ENUM = ClassName.get(RPC_PACKAGE, "Enum");
    public static final ClassName MESSAGE = ClassName.get(RPC_PACKAGE, "Message");
    public static final ClassName FIELD = ClassName.get(RPC_PACKAGE, "Field");
    public static final ClassName RESPONSE = MESSAGE.nestedClass("Response");
    public static final ClassName STORE_REQ = MESSAGE.nestedClass("StoreRequest");

    public static final String SERIALIZE_PACKAGE = "io.dingodb.sdk.grpc.serializer";

    public static final String WRITE = "write";
    public static final String SIZE_OF = "sizeOf";

    public static final ClassName WRITER = ClassName.get(SERIALIZE_PACKAGE, "Writer");
    public static final ClassName READER = ClassName.get(SERIALIZE_PACKAGE, "Reader");
    public static final ClassName SIZE_UTILS = ClassName.get(SERIALIZE_PACKAGE, "SizeUtils");

    public static final ClassName MARSHALLER = ClassName.get(SERIALIZE_PACKAGE, "Marshaller");


}
