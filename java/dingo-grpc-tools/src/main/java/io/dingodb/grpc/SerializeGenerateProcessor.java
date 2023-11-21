package io.dingodb.grpc;

import com.squareup.javapoet.ArrayTypeName;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.CodeBlock;
import com.squareup.javapoet.ParameterizedTypeName;
import com.squareup.javapoet.TypeName;

import java.util.List;

import static com.google.common.base.CaseFormat.LOWER_CAMEL;
import static com.google.common.base.CaseFormat.LOWER_UNDERSCORE;
import static io.dingodb.grpc.Constant.INPUT;
import static io.dingodb.grpc.Constant.NUMBER;
import static io.dingodb.grpc.Constant.OUT;
import static io.dingodb.grpc.Constant.READER;
import static io.dingodb.grpc.Constant.SIZE_OF;
import static io.dingodb.grpc.Constant.SIZE_UTILS;
import static io.dingodb.grpc.Constant.VALUE;
import static io.dingodb.grpc.Constant.WRITE;
import static io.dingodb.grpc.Constant.WRITER;


public class SerializeGenerateProcessor {

    public static final CodeBlock DIRECT_SERIALIZE = CodeBlock.of(
            "$T.$L($L(), $L(), $L)", WRITER, WRITE, NUMBER, VALUE, OUT
        );

    public static final CodeBlock DIRECT_SERIALIZE_SIZE = CodeBlock.builder()
        .add(
            "return $T.$L($L(), $L())", SIZE_UTILS, SIZE_OF, NUMBER, VALUE
        )
        .build();

    public static final CodeBlock MESSAGE_SERIALIZE = CodeBlock.builder()
        .add("super.$L($L)", WRITE, OUT)
        .build();

    public static final CodeBlock MESSAGE_SERIALIZE_SIZE = CodeBlock.builder()
        .add("return super.$L()", SIZE_OF)
        .build();

    public static final CodeBlock PACK_SERIALIZE = CodeBlock.builder()
        .add(
            "$T.$L($L(), $L(), $L, $T::$L, $T::$L)", WRITER, WRITE, NUMBER, VALUE, OUT, WRITER, WRITE, SIZE_UTILS, SIZE_OF
        )
        .build();

    public static final CodeBlock PACK_SERIALIZE_SIZE = CodeBlock.builder()
        .add(
            "return $T.$L($L(), $L(), $L, $T::$L, $T::$L)", WRITER, WRITE, NUMBER, VALUE, OUT, WRITER, WRITE, SIZE_UTILS, SIZE_OF
        )
        .build();

    public static final CodeBlock REPEATED_SERIALIZE = CodeBlock.builder()
        .add(
            "$T.$L($L(), $L(), (n, v) -> $T.$L(n, v, $L))", WRITER, WRITE, NUMBER, VALUE, WRITER, WRITE, OUT
        )
        .build();

    public static final CodeBlock REPEATED_SERIALIZE_SIZE = CodeBlock.builder()
        .add(
            "return $T.$L($L(), $L(), $T::$L)", SIZE_UTILS, SIZE_OF, NUMBER, VALUE, SIZE_UTILS, SIZE_OF
        )
        .build();

    public static final CodeBlock MAP_SERIALIZE = CodeBlock.builder()
        .add(
            "$T.$L($L(), $L(), $L, (n, v) -> $T.$L(n, v, $L), (n, v) -> $T.$L(n, v, $L), $T::$L, $T::$L)", WRITER, WRITE, NUMBER, VALUE, OUT,
            WRITER, WRITE, OUT, WRITER, WRITE, OUT,
            SIZE_UTILS, SIZE_OF,
            SIZE_UTILS, SIZE_OF
        )
        .build();

    public static final CodeBlock MAP_SERIALIZE_SIZE = CodeBlock.builder()
        .add(
            "return $T.$L($L(), $L(), $T::$L, $T::$L, $T::$L, $T::$L)",
            SIZE_UTILS, SIZE_OF, NUMBER, VALUE, WRITER, WRITE, WRITER, WRITE,
            SIZE_UTILS, SIZE_OF,
            SIZE_UTILS, SIZE_OF
        )
        .build();

    private static boolean canPack(TypeName typeName) {
        return typeName.isPrimitive() || typeName.isBoxedPrimitive();
    }

    public static boolean canDirect(TypeName typeName) {
        return typeName.isPrimitive()
            || typeName.isBoxedPrimitive()
            || typeName.equals(ClassName.get(String.class))
            || typeName.equals(ArrayTypeName.of(byte.class));
    }

    public static String directRead(TypeName typeName) {
        if (typeName instanceof ArrayTypeName) {
            return "readBytes";
        }
        if (typeName.equals(TypeName.get(String.class))) {
            return "readString";
        }
        return LOWER_UNDERSCORE.to(LOWER_CAMEL, "read_" + typeName.unbox());
    }

    public static CodeBlock readStatement(TypeName typeName) {
        if (canDirect(typeName)) {
            return CodeBlock.of(
                "$L($T.$L($L))",
                VALUE, READER, directRead(typeName), INPUT
            );
        }
        if (typeName instanceof ParameterizedTypeName) {
            TypeName firstGenericType = ((ParameterizedTypeName) typeName).typeArguments.get(0);
            if (((ParameterizedTypeName) typeName).rawType.equals(ClassName.get(List.class))) {
                if (canPack(firstGenericType)) {
                    return CodeBlock.of(
                        "$L($T.readPack($L, $T::$L))",
                        VALUE, READER, INPUT, READER, directRead(firstGenericType)
                    );
                } else if (canDirect(firstGenericType)) {
                    return CodeBlock.of(
                            "$L($T.readList($L(), $L, $T::$L))",
                            VALUE, READER, VALUE, INPUT, READER, directRead(firstGenericType)
                        );
                } else {
                    return CodeBlock.of(
                        "$L($T.readList($L, $L, in -> $T.readMessage(new $T(), in)))",
                        VALUE, READER, VALUE, INPUT, READER, firstGenericType
                    );
                }
            } else {
                if (canDirect(((ParameterizedTypeName) typeName).typeArguments.get(1))) {
                    String keyReader = directRead(firstGenericType);
                    String valueReader = directRead(((ParameterizedTypeName) typeName).typeArguments.get(1));
                    return CodeBlock.of(
                        "$L($T.readMap($L, $L, $L, $T::$L, $T::$L))",
                        VALUE, READER, NUMBER, VALUE, INPUT, READER, keyReader, READER, valueReader
                    );
                } else {
                    String keyReader = directRead(firstGenericType);
                    TypeName valueType = ((ParameterizedTypeName) typeName).typeArguments.get(1);
                    return CodeBlock.of(
                        "$L($T.readMap($L, $L, $L, $T::$L, in -> $T.readMessage(new $T(), in)))",
                        VALUE, READER, NUMBER, VALUE, INPUT, READER, keyReader, READER, valueType
                    );
                }
            }
        } else {
            return CodeBlock.of("$T.readMessage($L(new $T()).$L(), $L)", READER, VALUE, typeName, VALUE, INPUT);
        }
    }

    public static CodeBlock writeStatement(TypeName typeName) {
        if (canDirect(typeName)) {
            return DIRECT_SERIALIZE;
        }
        if (typeName instanceof ParameterizedTypeName) {
            if (((ParameterizedTypeName) typeName).rawType.equals(ClassName.get(List.class))) {
                if (canPack(((ParameterizedTypeName) typeName).typeArguments.get(0))) {
                    return PACK_SERIALIZE;
                }
                return REPEATED_SERIALIZE;
            } else {
                return MAP_SERIALIZE;
            }
        } else {
            return MESSAGE_SERIALIZE;
        }
    }

    public static CodeBlock sizeOfStatement(TypeName typeName) {
        if (canDirect(typeName)) {
            return DIRECT_SERIALIZE_SIZE;
        }
        if (typeName instanceof ParameterizedTypeName) {
            if (((ParameterizedTypeName) typeName).rawType.equals(ClassName.get(List.class))) {
                if (canPack(typeName)) {
                    return PACK_SERIALIZE_SIZE;
                }
                return REPEATED_SERIALIZE_SIZE;
            } else {
                return MAP_SERIALIZE_SIZE;
            }
        } else {
            return MESSAGE_SERIALIZE_SIZE;
        }
    }
}
