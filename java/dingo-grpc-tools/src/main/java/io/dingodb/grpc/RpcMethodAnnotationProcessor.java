/*
 * Copyright 2021 DataCanvas
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package io.dingodb.grpc;

import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.FieldSpec;
import com.squareup.javapoet.JavaFile;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.ParameterizedTypeName;
import com.squareup.javapoet.TypeName;
import com.squareup.javapoet.TypeSpec;
import io.grpc.MethodDescriptor;
import io.grpc.stub.annotations.RpcMethod;
import lombok.AllArgsConstructor;
import lombok.Getter;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.annotation.processing.AbstractProcessor;
import javax.annotation.processing.ProcessingEnvironment;
import javax.annotation.processing.RoundEnvironment;
import javax.annotation.processing.SupportedSourceVersion;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.AnnotationMirror;
import javax.lang.model.element.AnnotationValue;
import javax.lang.model.element.Element;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.TypeElement;
import javax.lang.model.type.TypeMirror;
import javax.lang.model.util.Elements;
import javax.lang.model.util.Types;
import javax.tools.FileObject;
import javax.tools.StandardLocation;

import static io.dingodb.grpc.Constant.CALLER;
import static io.dingodb.grpc.Constant.COMMON_ID;
import static io.dingodb.grpc.Constant.MARSHALLER;
import static io.dingodb.grpc.Constant.MSG_PKG;
import static io.grpc.MethodDescriptor.MethodType.UNARY;
import static javax.lang.model.element.Modifier.ABSTRACT;
import static javax.lang.model.element.Modifier.DEFAULT;
import static javax.lang.model.element.Modifier.FINAL;
import static javax.lang.model.element.Modifier.PUBLIC;
import static javax.lang.model.element.Modifier.STATIC;

@SupportedSourceVersion(SourceVersion.RELEASE_8)
public class RpcMethodAnnotationProcessor extends AbstractProcessor {

    private static final String fullMethodName = "fullMethodName";
    private static final String requestType = "requestType";
    private static final String responseType = "responseType";
    private static final String methodType = "methodType";

    private Types types;
    private Elements elements;
    private MessageGenerateProcessor messageGenerateProcessor;

    private boolean processed = false;

    @Override
    public Set<String> getSupportedAnnotationTypes() {
        return Collections.singleton(Constant.GRPC_ANNOTATION);
    }

    @Override
    public synchronized void init(ProcessingEnvironment processingEnv) {
        super.init(processingEnv);
        this.types = processingEnv.getTypeUtils();
        this.elements = processingEnv.getElementUtils();
        this.messageGenerateProcessor = new MessageGenerateProcessor(this.types, this.elements);
    }

    @Override
    public boolean process(Set<? extends TypeElement> annotations, RoundEnvironment roundEnv) {
        if (processed) {
            return true;
        }
        Map<ClassName, TypeSpec.Builder> rpcs = new HashMap<>();
        Set<FileObject> sources = new TreeSet<>(Comparator.comparing(FileObject::getName));
        Set<TypeElement> serviceElements = new TreeSet<>(Comparator.comparing(Element::toString));
        Set<TypeElement> entityElements = new TreeSet<>(Comparator.comparing(Element::toString));
        for (Element element : roundEnv.getElementsAnnotatedWith(RpcMethod.class)) {
            if (!(element instanceof ExecutableElement)) {
                continue;
            }
            AnnotationMirror annotationMirror = filter(element);
            if (annotationMirror == null) {
                continue;
            }
            Map<String, AnnotationValue> annotationValues = getAnnotationValues(annotationMirror);
            String name = element.getEnclosingElement().getSimpleName().toString();
            name = name.substring(0, name.length() - 4);
            ClassName className = ClassName.get(Constant.SERVICE_PKG, name);
            TypeSpec.Builder typeBuilder = rpcs.computeIfAbsent(className, TypeSpec::interfaceBuilder);
            String methodName = cleanFullMethodName(annotationValues.get(fullMethodName).toString());
            try {
                TypeElement reqTypeElement = (TypeElement) types.asElement((TypeMirror) annotationValues.get(requestType).getValue());
                TypeElement resTypeElement = (TypeElement) types.asElement((TypeMirror) annotationValues.get(responseType).getValue());
                ClassName reqTypeName = messageGenerateProcessor.generateMessage(reqTypeElement);
                ClassName resTypeName = messageGenerateProcessor.generateMessage(resTypeElement);
                typeBuilder.addField(makeMethodFieldSpec(
                    methodName,
                    annotationValues.get(fullMethodName).toString(), reqTypeName, resTypeName
                ));
                typeBuilder
                    .addMethod(makeRequestMethod(methodName, reqTypeName, resTypeName))
                    .addMethod(makeProviderMethod(methodName, reqTypeName, resTypeName))
                    .addMethod(makeRequestWithIdMethod(methodName, reqTypeName, resTypeName))
                    .addMethod(makeProviderWithIdMethod(methodName, reqTypeName, resTypeName));
                messageGenerateProcessor.messages.get(reqTypeName).addSuperinterface(Constant.REQUEST);
                messageGenerateProcessor.messages.get(resTypeName).addSuperinterface(Constant.RESPONSE);
                if (elements.getPackageOf(reqTypeElement).getSimpleName().toString().equals("store")) {
                    messageGenerateProcessor.messages.get(reqTypeName).addSuperinterface(Constant.STORE_REQ);
                }
                if (elements.getPackageOf(reqTypeElement).getSimpleName().toString().equals("index")) {
                    messageGenerateProcessor.messages.get(reqTypeName).addSuperinterface(Constant.STORE_REQ);
                }
                serviceElements.add((TypeElement) element.getEnclosingElement());
                entityElements.add((TypeElement) reqTypeElement.getEnclosingElement());
            } catch (Exception e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }

        }
        try {
            entityElements.stream()
                .flatMap(element -> element.getEnclosedElements().stream())
                .filter(element -> element.getKind() == ElementKind.ENUM)
                .map(TypeElement.class::cast)
                .forEach(element -> {
                    messageGenerateProcessor.generateEnum(ClassName.get(
                        MSG_PKG + "." + messageGenerateProcessor.packageOf(element), element.getSimpleName().toString()
                    ), element);
                });
            for (Map.Entry<ClassName, TypeSpec.Builder> entry : messageGenerateProcessor.messages.entrySet()) {
                createJavaFile(entry.getKey().packageName(), entry.getValue().build());
            }
            for (Map.Entry<ClassName, TypeSpec.Builder> entry : rpcs.entrySet()) {
                TypeSpec.Builder builder = entry.getValue();

                ParameterizedTypeName caller = ParameterizedTypeName.get(CALLER, entry.getKey());
                TypeSpec implType = TypeSpec.classBuilder("Impl")
                    .addSuperinterface(entry.getKey())
                    .addAnnotation(Getter.class)
                    .addAnnotation(AllArgsConstructor.class)
                    .addField(caller, "caller", PUBLIC, FINAL)
                    .addModifiers(PUBLIC, STATIC)
                    .build();

                builder
                    .addType(implType)
                    .addMethod(MethodSpec
                        .methodBuilder("getCaller")
                        .addModifiers(PUBLIC, ABSTRACT)
                        .returns(caller)
                        .build()
                    )
                    .addSuperinterface(ParameterizedTypeName.get(Constant.SERVICE, entry.getKey()))
                    .addModifiers(PUBLIC);

                createJavaFile(Constant.SERVICE_PKG, builder.build());
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
        return processed = true;
    }

    private FileObject getSourceFilePathForElement(Element element) throws IOException {
        String packageName = processingEnv.getElementUtils().getPackageOf(element).getQualifiedName().toString();
        String className = element.getSimpleName().toString();
        String fileName = className + ".java";

        return processingEnv.getFiler().getResource(StandardLocation.SOURCE_PATH, packageName, fileName);
    }

    private FieldSpec makeMethodFieldSpec(String name, String fullMethodName, TypeName req, TypeName res) {
        ParameterizedTypeName type = ParameterizedTypeName.get(ClassName.get(MethodDescriptor.class), req, res);
        return FieldSpec.builder(type, name, PUBLIC, STATIC, FINAL)
            .initializer(
                "$T.newBuilder()\n" +
                    "    .setType($T.$L)\n" +
                    "    .setFullMethodName($L)\n" +
                    "    .setSampledToLocalTracing(true)\n" +
                    "    .setRequestMarshaller(new $L($T::new))\n" +
                    "    .setResponseMarshaller(new $L($T::new))" +
                    "    .build()",
                MethodDescriptor.class,
                MethodDescriptor.MethodType.class, UNARY,
                fullMethodName,
                MARSHALLER, req,
                MARSHALLER, res
            )
            .build();
    }

    private MethodSpec makeRequestMethod(
        String methodName,
        TypeName requestTypeName,
        TypeName responseTypeName
    ) {

        return MethodSpec.methodBuilder(methodName)
            .returns(responseTypeName)
            .addParameter(requestTypeName, "request")
            .addModifiers(PUBLIC, DEFAULT)
            .addStatement("return getCaller().call($L, request)", methodName)
            .build();
    }

    private MethodSpec makeRequestWithIdMethod(
        String methodName,
        TypeName requestTypeName,
        TypeName responseTypeName
    ) {

        return MethodSpec.methodBuilder(methodName)
            .returns(responseTypeName)
            .addParameter(TypeName.LONG, "requestId")
            .addParameter(requestTypeName, "request")
            .addModifiers(PUBLIC, DEFAULT)
            .addStatement("return getCaller().call($L, requestId, request)", methodName)
            .build();
    }

    private MethodSpec makeProviderMethod(
        String methodName,
        TypeName requestTypeName,
        TypeName responseTypeName
    ) {
        return MethodSpec.methodBuilder(methodName)
            .returns(responseTypeName)
            .addParameter(ParameterizedTypeName.get(ClassName.get(Supplier.class), requestTypeName), "provider")
            .addModifiers(PUBLIC, DEFAULT)
            .addStatement("return getCaller().call($L, provider)", methodName)
            .build();
    }

    private MethodSpec makeProviderWithIdMethod(
        String methodName,
        TypeName requestTypeName,
        TypeName responseTypeName
    ) {
        return MethodSpec.methodBuilder(methodName)
            .returns(responseTypeName)
            .addParameter(TypeName.LONG, "requestId")
            .addParameter(ParameterizedTypeName.get(ClassName.get(Supplier.class), requestTypeName), "provider")
            .addModifiers(PUBLIC, DEFAULT)
            .addStatement("return getCaller().call($L, requestId, provider)", methodName)
            .build();
    }

    private String cleanFullMethodName(String fullMethodName) {
        int sepIndex = fullMethodName.lastIndexOf("/") + 1;
        return Character.toLowerCase(fullMethodName.charAt(sepIndex))
             + fullMethodName.substring(sepIndex + 1, fullMethodName.length() - 1);
    }

    private Map<String, AnnotationValue> getAnnotationValues(AnnotationMirror annotationMirror) {
        return annotationMirror.getElementValues().entrySet().stream().collect(Collectors.toMap(
            e -> e.getKey().getSimpleName().toString(), Map.Entry::getValue
        ));
    }

    private AnnotationMirror filter(Element element) {
        return element.getAnnotationMirrors().stream()
            .filter(am -> am.getAnnotationType().toString().equals(Constant.GRPC_ANNOTATION))
            .findAny().orElse(null);
    }

    private void createJavaFile(String packageName, TypeSpec typeSpec) throws IOException {

        JavaFile javaFile = JavaFile.builder(packageName, typeSpec)
            .indent("    ")
            .skipJavaLangImports(true)
            .build();
        javaFile.writeTo(processingEnv.getFiler());

    }

}
