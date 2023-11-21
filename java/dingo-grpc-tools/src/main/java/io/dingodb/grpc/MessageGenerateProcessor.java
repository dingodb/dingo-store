package io.dingodb.grpc;


import com.google.common.base.CaseFormat;
import com.google.protobuf.ByteString;
import com.google.protobuf.MapField;
import com.squareup.javapoet.ArrayTypeName;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.CodeBlock;
import com.squareup.javapoet.FieldSpec;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.ParameterSpec;
import com.squareup.javapoet.ParameterizedTypeName;
import com.squareup.javapoet.TypeName;
import com.squareup.javapoet.TypeSpec;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.Delegate;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import java.util.stream.Collectors;
import javax.lang.model.element.Element;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.Modifier;
import javax.lang.model.element.TypeElement;
import javax.lang.model.element.VariableElement;
import javax.lang.model.type.DeclaredType;
import javax.lang.model.type.TypeKind;
import javax.lang.model.type.TypeMirror;
import javax.lang.model.util.Elements;
import javax.lang.model.util.Types;

import static com.google.common.base.CaseFormat.LOWER_CAMEL;
import static com.google.common.base.CaseFormat.UPPER_CAMEL;
import static com.google.common.base.CaseFormat.UPPER_UNDERSCORE;
import static com.squareup.javapoet.TypeName.INT;
import static com.squareup.javapoet.TypeName.VOID;
import static io.dingodb.grpc.Constant.CASE_END;
import static io.dingodb.grpc.Constant.ENUM;
import static io.dingodb.grpc.Constant.FIELD;
import static io.dingodb.grpc.Constant.FIELD_END;
import static io.dingodb.grpc.Constant.FIELD_NUMBER_END;
import static io.dingodb.grpc.Constant.INPUT;
import static io.dingodb.grpc.Constant.INPUT_ARG;
import static io.dingodb.grpc.Constant.MESSAGE;
import static io.dingodb.grpc.Constant.NUMBER;
import static io.dingodb.grpc.Constant.OUT;
import static io.dingodb.grpc.Constant.OUT_ARG;
import static io.dingodb.grpc.Constant.READER;
import static io.dingodb.grpc.Constant.SIZE_OF;
import static io.dingodb.grpc.Constant.VALUE;
import static io.dingodb.grpc.Constant.WRITE;
import static io.dingodb.grpc.SerializeGenerateProcessor.canDirect;
import static io.dingodb.grpc.SerializeGenerateProcessor.directRead;
import static io.dingodb.grpc.SerializeGenerateProcessor.readStatement;
import static io.dingodb.grpc.SerializeGenerateProcessor.sizeOfStatement;
import static io.dingodb.grpc.SerializeGenerateProcessor.writeStatement;
import static java.util.Optional.ofNullable;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;
import static javax.lang.model.element.Modifier.ABSTRACT;
import static javax.lang.model.element.Modifier.FINAL;
import static javax.lang.model.element.Modifier.PRIVATE;
import static javax.lang.model.element.Modifier.PUBLIC;
import static javax.lang.model.element.Modifier.STATIC;


public class MessageGenerateProcessor {

    private final Types types;
    private final Elements elements;

    protected final Map<ClassName, TypeSpec.Builder> messages = new HashMap<>();
    private final Map<ClassName, TypeSpec.Builder> enumMessages = new HashMap<>();

    public MessageGenerateProcessor(Types types, Elements elements) {
        this.types = types;
        this.elements = elements;
    }

    public String packageOf(Element element) {
        return elements.getPackageOf(types.asElement(element.asType())).getSimpleName().toString();
    }

    public boolean exist(ClassName className) {
        return messages.containsKey(className) || elements.getTypeElement(className.canonicalName()) != null;
    }

    public ClassName generateMessage(TypeElement element) {
        return generateMessage(null, element);
    }

    private TypeName fieldType(String name, Element element, Map<String, ? extends Element> parentElements) {

        if (element.asType().getKind().isPrimitive()) {
            TypeMirror primitiveType = element.asType();
            if (primitiveType.getKind() == TypeKind.INT) {
                element = this.types.asElement(((ExecutableElement) parentElements.get(
                    UPPER_UNDERSCORE.to(LOWER_CAMEL, "GET_" + LOWER_CAMEL.to(UPPER_UNDERSCORE, name) + "()")
                )).getReturnType());
                if (element != null && element.getKind() == ElementKind.ENUM) {
                    ClassName className = ClassName.get(
                        Constant.MESSAGE_PACKAGE + "." + packageOf(element), element.getSimpleName().toString());
                    generateEnum(className, (TypeElement) element);
                    return className;
                }
            }
            return ClassName.get(primitiveType);
        }
        if (element.asType().toString().equals(Object.class.getName())) {
            element = this.types.asElement(((ExecutableElement) parentElements.get(
                UPPER_UNDERSCORE.to(LOWER_CAMEL, "GET_" + LOWER_CAMEL.to(UPPER_UNDERSCORE, name) + "()")
            )).getReturnType());
            return fieldType(name, element, parentElements);
        }

        if (ByteString.class.getName().equals(element.asType().toString())) {
            return ArrayTypeName.of(byte.class);
        }

        if (Object.class.getPackage().getName().equals(elements.getPackageOf(element).toString())) {
            return ClassName.get(element.asType());
        }

        if (types.isAssignable(
            types.erasure(element.asType()), types.erasure(elements.getTypeElement(List.class.getName()).asType())
        )) {

            ClassName className = ClassName.get(List.class);
            DeclaredType declaredType = (DeclaredType) element.asType();
            while (declaredType.getTypeArguments().size() < 1) {
                declaredType = (DeclaredType) types.directSupertypes(declaredType).get(1);
            }
            TypeName arg = fieldType(null, types.asElement(declaredType.getTypeArguments().get(0)), parentElements);
            return ParameterizedTypeName.get(className, arg);
        }

        if (types.isAssignable(
            types.erasure(element.asType()), types.erasure(elements.getTypeElement(Map.class.getName()).asType())
        ) || types.isAssignable(
            types.erasure(element.asType()), types.erasure(elements.getTypeElement(MapField.class.getName()).asType())
        )) {

            ClassName className = ClassName.get(Map.class);
            DeclaredType declaredType = (DeclaredType) element.asType();
            while (declaredType.getTypeArguments().size() < 2) {
                declaredType = (DeclaredType) types.directSupertypes(declaredType).get(1);
            }
            TypeName arg1 = fieldType(null, types.asElement(declaredType.getTypeArguments().get(0)), parentElements);
            TypeName arg2 = fieldType(null, types.asElement(declaredType.getTypeArguments().get(1)), parentElements);
            return ParameterizedTypeName.get(className, arg1, arg2);
        }

        element = types.asElement(element.asType());
        ClassName className = ClassName.get(Constant.MESSAGE_PACKAGE + "." + packageOf(element), element.getSimpleName().toString());
        generateMessage(className, (TypeElement) element);
        return className;
    }

    private String numberStatement(int number) {
        return "return " + (number == 0 ? "value().number()" : number);
    }

    private TypeSpec generateField(TypeName className, TypeName typeName, int number) {
        CodeBlock readStatement;
        TypeSpec.Builder builder = TypeSpec.classBuilder((ClassName) className);
        if (enumMessages.containsKey(typeName)) {
            readStatement = CodeBlock.of(
                "$L($T.forNumber($T.readInt($L)))",
                VALUE, typeName, READER, INPUT
            );
            builder.addMethod(MethodSpec.constructorBuilder()
                .addModifiers(PUBLIC)
                .addStatement("$L = $T.forNumber(0)", VALUE, typeName)
                .build());
        } else if (number == 0) {
            readStatement = CodeBlock.of("throw new $T()", UnsupportedOperationException.class);
        } else {
            readStatement = readStatement(typeName);
        }
        return builder
            .addModifiers(PUBLIC)
            .superclass(ParameterizedTypeName.get(FIELD, typeName))
            .addField(FieldSpec.builder(int.class, NUMBER, PUBLIC, STATIC, FINAL).initializer("$L", number).build())
            .addMethod(overrideBuilder(NUMBER, INT.box()).addStatement(numberStatement(number)).build())
            .addMethod(overrideBuilder(WRITE, VOID, OUT_ARG).addStatement(writeStatement(typeName)).build())
            .addMethod(overrideBuilder(SIZE_OF, INT.box()).addStatement(sizeOfStatement(typeName)).build())
            .addMethod(overrideBuilder("read", VOID, INPUT_ARG).addStatement(readStatement).build())
            .addMethod(overrideBuilder("toString", TypeName.get(String.class))
                .addStatement("return $T.valueOf($L())", String.class, VALUE).build())
            .build();
    }

    private void generateEnum(ClassName className, TypeElement element) {
        TypeSpec.Builder builder = TypeSpec.enumBuilder(className).addModifiers(PUBLIC)
            .addSuperinterface(ENUM)
            .addField(Integer.class, NUMBER, PUBLIC, FINAL)
            .addMethod(MethodSpec.constructorBuilder()
                .addModifiers(PRIVATE)
                .addParameter(Integer.class, NUMBER)
                .addStatement("this.$L = $L", NUMBER, NUMBER)
                .build())
            .addMethod(overrideBuilder(NUMBER, INT)
                .addStatement("return $L", NUMBER)
                .build()
            );
        Map<String, Element> elements = element
            .getEnclosedElements().stream()
            .collect(toMap(Object::toString, __ -> __));
        MethodSpec.Builder forNumberMethod = methodBuilder("forNumber", className, PUBLIC, STATIC).addParameter(INT, "number");
        forNumberMethod.beginControlFlow("switch(number)");
        for (String name : elements.keySet()) {
            if (!(elements.get(name).getKind() == ElementKind.ENUM_CONSTANT)) {
                continue;
            }
            VariableElement variableElement = (VariableElement) elements.get(name + "_VALUE");
            Integer number = variableElement == null ? -1 : (Integer) variableElement.getConstantValue();
            builder.addEnumConstant(name, TypeSpec.anonymousClassBuilder("$L", number).build());
            forNumberMethod.addStatement("case $L: return $L", number, name);
        }
        forNumberMethod.addStatement("default: return null");
        forNumberMethod.endControlFlow();
        builder.addMethod(forNumberMethod.build());
        messages.put(className, builder);
        enumMessages.put(className, builder);
    }

    private ClassName generateMessage(ClassName className, TypeElement element) {

        if (className != null && exist(className)) {
            return className;
        }

        ClassName newClassName = className;
        if (newClassName == null) {
            newClassName = ClassName.get(Constant.MESSAGE_PACKAGE + "." + packageOf(element), element.getSimpleName().toString());
        }

        TypeSpec.Builder builder = TypeSpec.classBuilder(newClassName);

        List<FieldSpec> fields = new ArrayList<>();
        List<MethodSpec> methods = new ArrayList<>();
        List<TypeSpec> fieldTypes = new ArrayList<>();
        Map<String, TypeSpec> oneOfTypes = new HashMap<>();


        Map<String, Element> elements = element.getEnclosedElements().stream().collect(toMap(Object::toString, identity()));
        for (String name : new ArrayList<>(elements.keySet())) {
            if (name.endsWith(FIELD_NUMBER_END)) {
                String numberName = UPPER_UNDERSCORE.to(
                    LOWER_CAMEL, name.substring(0, name.length() - FIELD_NUMBER_END.length())
                ).toUpperCase();
                elements.put(numberName, elements.get(name));
            }
        }
        for (String name : elements.keySet()) {
            if (!name.endsWith(FIELD_END)) {
                continue;
            }
            if (name.endsWith("converter_")
                && elements.containsKey(name.substring(0, name.length() - "converter_".length()))
            ) {
                continue;
            }
            if (name.endsWith(CASE_END)
                && elements.containsKey(name.substring(0, name.length() - CASE_END.length()) + "_")
            ) {
                continue;
            }
            String fieldName = name.substring(0, name.length() - 1);

            Element fieldElement = elements.get(name);
            VariableElement numberField = (VariableElement) elements.get(fieldName.toUpperCase());
            int number = 0;

            TypeName fieldType;
            if (numberField == null) {
                // one of message
                TypeElement oneOfEnum = this.elements.getTypeElement(
                    element.getQualifiedName().toString() + "."
                        + LOWER_CAMEL.to(CaseFormat.UPPER_CAMEL, fieldName + "Case")
                );
                fieldType = newClassName.nestedClass(LOWER_CAMEL.to(UPPER_CAMEL, fieldName + "Nest"));
                TypeSpec oneOfType = generateOneOfMessage((ClassName) fieldType, oneOfEnum, elements);
                builder.addType(oneOfType);
                oneOfTypes.put(fieldName, oneOfType);
            } else {
                number = (Integer) numberField.getConstantValue();
                fieldType = fieldType(fieldName, fieldElement, elements).box();
            }

            ClassName realName = newClassName.nestedClass(LOWER_CAMEL.to(UPPER_CAMEL, fieldName) + "Field");

            fieldTypes.add(generateField(realName, fieldType, number));

            fields.add(FieldSpec.builder(realName, fieldName, PRIVATE, FINAL).initializer("new $T()", realName).build());

            methods.add(methodBuilder(fieldName, newClassName, PUBLIC)
                .addParameter(fieldType, fieldName)
                .addStatement("this.$L.value($L)", fieldName, fieldName)
                .addStatement("return this")
                .build()
            );

            methods.add(methodBuilder(fieldName, fieldType, PUBLIC)
                .addStatement("return this.$L.value()", fieldName).build()
            );

        }

        MethodSpec.Builder toStringBuilder = overrideBuilder("toString", ClassName.get(String.class));
        toStringBuilder.addStatement(
            "$T strJoiner = new $T(\", \", \"$L: {\", \"}\")",
            StringJoiner.class, StringJoiner.class, newClassName.simpleName()
        );
        for (FieldSpec field : fields) {
            toStringBuilder.addStatement("strJoiner.add(\"$L = \" + $L.toString())", field.name, field.name);
        }
        toStringBuilder.addStatement("return strJoiner.toString()");

        builder
            .addModifiers(PUBLIC)
            .addAnnotation(Getter.class).addAnnotation(EqualsAndHashCode.class)
            .addAnnotation(ToString.class)
            .addSuperinterface(MESSAGE)
            .addFields(fields)
            .addTypes(fieldTypes)
            .addMethods(methods)
            .addMethod(toStringBuilder.build())
            .addMethod(makeWriteMethod(fields))
            .addMethod(makeSizeOfMethod(fields))
            .addMethod(makeReadMethod(fields, oneOfTypes));
        messages.put(newClassName, builder);

        return newClassName;
    }

    private TypeSpec generateOneOfMessage(
        ClassName className, TypeElement element, Map<String, ? extends Element> parentElements
    ) {

        List<? extends Element> oneOfElement = element.getEnclosedElements().stream()
            .filter(__ -> __.getKind() == ElementKind.ENUM_CONSTANT)
            .collect(Collectors.toList());

        ClassName nest = className.nestedClass("Nest");
        TypeSpec.Builder builder = TypeSpec.interfaceBuilder(className).addModifiers(PUBLIC)
            .addMethod(methodBuilder(NUMBER, INT.box(), PUBLIC, ABSTRACT).build())
            .addMethod(methodBuilder("nest", nest, PUBLIC, ABSTRACT).build());

        TypeSpec.Builder nestBuilder = TypeSpec.enumBuilder(nest).addModifiers(PUBLIC, STATIC)
            .addSuperinterface(ENUM)
            .addField(Integer.class, NUMBER, PUBLIC, FINAL)
            .addMethod(MethodSpec.constructorBuilder()
                .addModifiers(PRIVATE)
                .addParameter(Integer.class, NUMBER)
                .addStatement("this.$L = $L", NUMBER, NUMBER)
                .build())
            .addMethod(overrideBuilder(NUMBER, INT)
                .addStatement("return $L", NUMBER)
                .build()
            );

        for (Element enumElement : oneOfElement) {
            String name = enumElement.toString();
            Object number = ofNullable(parentElements.get(name + FIELD_NUMBER_END))
                .map(__ -> ((VariableElement) __).getConstantValue()).orElse(null);
            if (name.endsWith("_NOT_SET") && number == null) {
                continue;
            }

            TypeMirror returnType = ((ExecutableElement) parentElements.get(
                UPPER_UNDERSCORE.to(LOWER_CAMEL, "GET_" + name) + "()")
            ).getReturnType();
            TypeName realTypeName;
            FieldSpec.Builder realValueField;
            if (returnType.getKind().isPrimitive()) {
                realTypeName = ClassName.get(returnType).box();
                realValueField = FieldSpec.builder(realTypeName, "realValue", PUBLIC);
            } else {
                TypeElement realType = (TypeElement) types.asElement(returnType);
                realTypeName = fieldType(null, realType, parentElements);
                realValueField = FieldSpec.builder(realTypeName, "realValue", PUBLIC);
                if (realTypeName instanceof ClassName) {
                    generateMessage((ClassName) realTypeName, realType);
                    realValueField.addAnnotation(Delegate.class);
                }
            }

            CodeBlock readStatement;
            if (canDirect(realTypeName)) {
                readStatement = CodeBlock.of("realValue = $T.$L($L)", READER, directRead(realTypeName), INPUT);
            } else {
                realValueField.initializer("new $T()", realTypeName);
                readStatement = CodeBlock.of("realValue.read(input)");
            }

            nestBuilder.addEnumConstant(name, TypeSpec.anonymousClassBuilder("$L", number).build());

            builder.addType(
                TypeSpec.classBuilder(UPPER_UNDERSCORE.to(UPPER_CAMEL, name))
                    .addAnnotation(Getter.class).addAnnotation(ToString.class)
                    .addAnnotation(AllArgsConstructor.class).addAnnotation(NoArgsConstructor.class)
                    .addSuperinterface(className)
                    .addModifiers(PUBLIC, STATIC)
                    .addField(realValueField.build())
                    .addField(FieldSpec.builder(INT, NUMBER, PUBLIC, STATIC, FINAL).initializer("$L", number).build())
                    .addMethod(methodBuilder("read", VOID, PUBLIC).addParameter(INPUT_ARG).addStatement(readStatement).build())
                    .addMethod(overrideBuilder(NUMBER, INT.box()).addStatement("return $L", number).build())
                    .addMethod(overrideBuilder("nest", nest).addStatement("return $T.$L", nest, name).build())
                    .build()
            );
        }

        return builder.addType(nestBuilder.build()).build();
    }

    private MethodSpec makeWriteMethod(List<FieldSpec> fields) {
        MethodSpec.Builder builder = overrideBuilder(WRITE, VOID, OUT_ARG);
        fields.forEach(field -> builder.addStatement("$L.$L($L)", field.name, WRITE, OUT));
        return builder.build();
    }

    private MethodSpec makeSizeOfMethod(List<FieldSpec> fields) {
        MethodSpec.Builder builder = overrideBuilder(SIZE_OF, TypeName.get(int.class));

        if (fields.size() == 0) {
            return builder.addStatement("return 0").build();
        }

        builder.addStatement("int size = 0");
        fields.forEach(field -> builder.addStatement("size += this.$L.$L()", field.name, SIZE_OF));
        builder.addStatement("return size");

        return builder.build();
    }

    private MethodSpec makeReadMethod(List<FieldSpec> fields, Map<String, TypeSpec> fieldTypes) {
        MethodSpec.Builder builder = overrideBuilder("read", VOID, INPUT_ARG);
        if (fields.size() == 0) {
            return builder.build();
        }
        builder.addStatement(CodeBlock.of("int number = 0"));
        builder.beginControlFlow("while((number = $T.$L($L)) != 0)", READER, "readNumber", INPUT);
        builder.beginControlFlow("switch(number)");
        for (FieldSpec field : fields) {
            String name = field.name;
            if (fieldTypes.containsKey(name)) {
                for (TypeSpec typeSpec : fieldTypes.get(name).typeSpecs) {
                    if (typeSpec.enumConstants.isEmpty()) {
                        ClassName type = ((ClassName) typeSpec.superinterfaces.get(0)).nestedClass(typeSpec.name);
                        builder.addStatement(
                            "case $T.$L: $L.$L(new $T()).read($L); break",
                            type, NUMBER, name, VALUE, type, INPUT
                        );
                    }
                }
            } else {
                builder.addStatement(
                    "case $T.$L: $L.read($L); break",
                    field.type, NUMBER, name, INPUT
                );
            }
        }
        builder.addStatement("default: $T.$L($L)", READER, "skip", INPUT);
        builder.endControlFlow();
        builder.endControlFlow();

        return builder.build();
    }

    private MethodSpec.Builder methodBuilder(String name, TypeName returnType, Modifier... modifiers) {
        return MethodSpec.methodBuilder(name).returns(returnType).addModifiers(modifiers);
    }

    private MethodSpec.Builder overrideBuilder(String name, TypeName returnType, ParameterSpec... parameter) {
        MethodSpec.Builder builder = methodBuilder(name, returnType, PUBLIC)
            .addAnnotation(Override.class)
            .returns(returnType);
        for (ParameterSpec parameterSpec : parameter) {
            builder.addParameter(parameterSpec);
        }
        return builder;
    }
}
