package io.dingodb.grpc;

import com.squareup.javapoet.ArrayTypeName;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.CodeBlock;
import com.squareup.javapoet.ParameterizedTypeName;
import com.squareup.javapoet.TypeName;

import java.util.List;
import java.util.Set;

import static com.google.common.base.CaseFormat.LOWER_CAMEL;
import static com.google.common.base.CaseFormat.LOWER_UNDERSCORE;
import static io.dingodb.grpc.Constant.HAS_VALUE;
import static io.dingodb.grpc.Constant.INPUT;
import static io.dingodb.grpc.Constant.NUMBER;
import static io.dingodb.grpc.Constant.OUT;
import static io.dingodb.grpc.Constant.READER;
import static io.dingodb.grpc.Constant.SIZE_OF;
import static io.dingodb.grpc.Constant.SIZE_UTILS;
import static io.dingodb.grpc.Constant.WRITE;
import static io.dingodb.grpc.Constant.WRITER;


public class SerializeGenerateProcessor {

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

    public static CodeBlock readStatement(String fieldName, TypeName fieldType, int number, Set<ClassName> enums) {
        if (canDirect(fieldType)) {
            return CodeBlock.of(
                "case $L: $L = $T.$L($L); $L = true; break",
                number, fieldName, READER, directRead(fieldType), INPUT, HAS_VALUE
            );
        }
        if (enums.contains(fieldType)) {
            return CodeBlock.of(
                "case $L: $L = $T.forNumber($T.readInt($L));$L = true; break",
                number, fieldName, fieldType, READER, INPUT, HAS_VALUE
            );
        }
        if (fieldType instanceof ParameterizedTypeName) {
            TypeName firstGenericType = ((ParameterizedTypeName) fieldType).typeArguments.get(0);
            if (((ParameterizedTypeName) fieldType).rawType.equals(ClassName.get(List.class))) {
                if (canPack(firstGenericType)) {
                    return CodeBlock.of(
                        "case $L: $L = $T.readPack($L, $T::$L); $L = true; break",
                        number, fieldName, READER, INPUT, READER, directRead(firstGenericType), HAS_VALUE
                    );
                } else if (canDirect(firstGenericType)) {
                    return CodeBlock.of(
                            "case $L: $L = $T.readList($L, $L, $T::$L); $L = true; break",
                            number, fieldName, READER, fieldName, INPUT, READER, directRead(firstGenericType), HAS_VALUE
                        );
                } else if (enums.contains(firstGenericType)) {
                    return CodeBlock.of(
                        "case $L: $L = $T.readList($L, $L, in -> $T.forNumber($T.readInt($L))); $L = true; break",
                        number, fieldName, READER, fieldName, INPUT, firstGenericType, READER, INPUT, HAS_VALUE
                    );
                } else {
                    return CodeBlock.of(
                        "case $L: $L = $T.readList($L, $L, in -> $T.readMessage(new $T(), in)); $L = true; break",
                        number, fieldName, READER, fieldName, INPUT, READER, firstGenericType, HAS_VALUE
                    );
                }
            } else {
                if (canDirect(((ParameterizedTypeName) fieldType).typeArguments.get(1))) {
                    String keyReader = directRead(firstGenericType);
                    String valueReader = directRead(((ParameterizedTypeName) fieldType).typeArguments.get(1));
                    return CodeBlock.of(
                        "case $L: $L = $T.readMap($L, $L, $L, $T::$L, $T::$L); $L = true; break",
                        number, fieldName, READER, NUMBER, fieldName, INPUT, READER, keyReader, READER, valueReader, HAS_VALUE
                    );
                } else if (enums.contains(firstGenericType)) {
                    String keyReader = directRead(firstGenericType);
                    TypeName valueType = ((ParameterizedTypeName) fieldType).typeArguments.get(1);
                    return CodeBlock.of(
                        "case $L: $L = $T.readMap($L, $L, $L, $T::$L, in -> in -> $T.forNumber($T.readInt(in))); $L = true; break",
                        number, fieldName, READER, NUMBER, fieldName, INPUT, READER, keyReader, READER, valueType, HAS_VALUE
                    );
                } else {
                    String keyReader = directRead(firstGenericType);
                    TypeName valueType = ((ParameterizedTypeName) fieldType).typeArguments.get(1);
                    return CodeBlock.of(
                        "case $L: $L = $T.readMap($L, $L, $L, $T::$L, in -> $T.readMessage(new $T(), in)); $L = true; break",
                        number, fieldName, READER, NUMBER, fieldName, INPUT, READER, keyReader, READER, valueType, HAS_VALUE
                    );
                }
            }
        } else {
            return CodeBlock.of(
                "case $L: $L = $T.readMessage(new $T(), $L); $L = $L ? $L : $L != null; break",
                number, fieldName, READER, fieldType, INPUT, HAS_VALUE, HAS_VALUE, HAS_VALUE, fieldName
            );
        }
    }

    public static CodeBlock writeStatement(String fieldName, TypeName fieldType, Integer number) {
        if (fieldType instanceof ParameterizedTypeName) {
            if (((ParameterizedTypeName) fieldType).rawType.equals(ClassName.get(List.class))) {
                if (canPack(((ParameterizedTypeName) fieldType).typeArguments.get(0))) {
                    return CodeBlock.builder()
                        .add(
                            "$T.$L($L, $L, $L, $T::$L, $T::$L)",
                            WRITER, WRITE, number, fieldName, OUT, WRITER, WRITE, SIZE_UTILS, SIZE_OF
                        )
                        .build();
                }
                return CodeBlock.builder()
                    .add(
                        "$T.$L($L, $L, (n, v) -> $T.$L(n, v, $L))",
                        WRITER, WRITE, number, fieldName, WRITER, WRITE, OUT
                    )
                    .build();
            } else {
                return CodeBlock.builder()
                    .add(
                        "$T.$L($L, $L, $L, (n, v) -> $T.$L(n, v, $L), (n, v) -> $T.$L(n, v, $L), $T::$L, $T::$L)",
                        WRITER, WRITE, number, fieldName, OUT,
                        WRITER, WRITE, OUT, WRITER, WRITE, OUT,
                        SIZE_UTILS, SIZE_OF,
                        SIZE_UTILS, SIZE_OF
                    )
                    .build();
            }
        } else {
            return CodeBlock.of(
                "$T.$L($L, $L, $L)", WRITER, WRITE, number, fieldName, OUT
            );
        }
    }

    public static CodeBlock sizeOfStatement(String fieldName, TypeName fieldType, Integer number) {
        if (fieldType instanceof ParameterizedTypeName) {
            if (((ParameterizedTypeName) fieldType).rawType.equals(ClassName.get(List.class))) {
                if (canPack(((ParameterizedTypeName) fieldType).typeArguments.get(0))) {
                    return CodeBlock.of(
                        "size += $T.$LPack($L, $L, $T::$L)",
                        SIZE_UTILS, SIZE_OF, number, fieldName, SIZE_UTILS, SIZE_OF
                    );
                }
                return CodeBlock.of(
                    "size += $T.$L($L, $L, $T::$L)",
                    SIZE_UTILS, SIZE_OF, number, fieldName, SIZE_UTILS, SIZE_OF
                );
            } else {
                return CodeBlock.of(
                    "size += $T.$L($L, $L, $T::$L, $T::$L)",
                    SIZE_UTILS, SIZE_OF, number, fieldName,
                    SIZE_UTILS, SIZE_OF,
                    SIZE_UTILS, SIZE_OF
                );
            }
        } else {
            return CodeBlock.of(
                "size += $T.$L($L, $L)", SIZE_UTILS, SIZE_OF, number, fieldName
            );
        }
    }
}
