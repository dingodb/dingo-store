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

import com.google.common.base.CaseFormat;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.FieldSpec;
import com.squareup.javapoet.JavaFile;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.ParameterSpec;
import com.squareup.javapoet.ParameterizedTypeName;
import com.squareup.javapoet.TypeName;
import com.squareup.javapoet.TypeSpec;
import io.grpc.MethodDescriptor;
import io.grpc.stub.annotations.RpcMethod;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
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

import static com.google.common.base.CaseFormat.LOWER_CAMEL;
import static com.google.common.base.CaseFormat.UPPER_CAMEL;
import static io.dingodb.grpc.Constant.CALLER;
import static io.dingodb.grpc.Constant.MSG_PKG;
import static io.dingodb.grpc.Constant.SERVICE_CALL_CYCLE;
import static io.dingodb.grpc.Constant.SERVICE_DESC_PKG;
import static io.dingodb.grpc.Constant.SERVICE_METHOD_BUILDER;
import static io.dingodb.grpc.Constant.SERVICE_PKG;
import static javax.lang.model.element.Modifier.ABSTRACT;
import static javax.lang.model.element.Modifier.DEFAULT;
import static javax.lang.model.element.Modifier.FINAL;
import static javax.lang.model.element.Modifier.PRIVATE;
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
        Map<ClassName, TypeSpec.Builder> rpcDescriptors = new HashMap<>();
        Map<ClassName, ClassName> descNames = new HashMap<>();
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
            ClassName serviceName = ClassName.get(SERVICE_PKG, name);
            ClassName serviceDescName = ClassName.get(
                SERVICE_DESC_PKG + "." + messageGenerateProcessor.packageOf(element), name + "Descriptors"
            );
            descNames.putIfAbsent(serviceName, serviceDescName);
            TypeSpec.Builder typeBuilder = rpcs.computeIfAbsent(serviceName, TypeSpec::interfaceBuilder);
            TypeSpec.Builder typeDescBuilder = rpcDescriptors.computeIfAbsent(
                serviceName, k -> TypeSpec.interfaceBuilder(serviceDescName)
            );
            String methodName = cleanFullMethodName(annotationValues.get(fullMethodName).toString());
            try {
                TypeElement reqTypeElement = (TypeElement) types.asElement((TypeMirror) annotationValues.get(requestType).getValue());
                TypeElement resTypeElement = (TypeElement) types.asElement((TypeMirror) annotationValues.get(responseType).getValue());
                ClassName reqTypeName = messageGenerateProcessor.generateMessage(reqTypeElement);
                ClassName resTypeName = messageGenerateProcessor.generateMessage(resTypeElement);
                typeDescBuilder.addField(makeMethodFieldSpec(
                    methodName,
                    annotationValues.get(fullMethodName).toString(), reqTypeName, resTypeName
                ));
                typeDescBuilder.addField(addHandlers(methodName, reqTypeName, resTypeName));
                typeDescBuilder.addType(addHandlerLogClass(serviceName, methodName));
                typeBuilder
                    .addMethod(makeRequestMethod(methodName, serviceDescName, reqTypeName, resTypeName))
                    .addMethod(makeRequestWithIdMethod(methodName, serviceDescName, reqTypeName, resTypeName));
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
                entry.getValue().addField(FieldSpec.builder(TypeName.OBJECT, "ext$", PRIVATE).build());
                createJavaFile(entry.getKey().packageName(), entry.getValue().build());
            }

            for (Map.Entry<ClassName, TypeSpec.Builder> entry : rpcDescriptors.entrySet()) {
                createJavaFile(
                    descNames.get(entry.getKey()).packageName(),
                    entry.getValue().addModifiers(PUBLIC).build()
                );
            }

            for (Map.Entry<ClassName, TypeSpec.Builder> entry : rpcs.entrySet()) {
                TypeSpec.Builder builder = entry.getValue();

                ClassName serviceName = entry.getKey();
                ParameterizedTypeName caller = ParameterizedTypeName.get(CALLER, serviceName);
                TypeSpec implType = TypeSpec.classBuilder("Impl")
                    .addSuperinterface(serviceName)
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
                    .addSuperinterface(ParameterizedTypeName.get(Constant.SERVICE, serviceName))
                    .addModifiers(PUBLIC);

                createJavaFile(
                    SERVICE_PKG, builder.build(), descNames.get(entry.getKey())
                );
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
                "$T.buildUnary($L, $T::new, $T::new)",
                SERVICE_METHOD_BUILDER, fullMethodName, req, res
            )
            .build();
    }

    private MethodSpec makeRequestMethod(
        String methodName, TypeName descTypeName, TypeName requestTypeName, TypeName responseTypeName
    ) {
        return MethodSpec.methodBuilder(methodName)
            .addAnnotation(Deprecated.class)
            .returns(responseTypeName)
            .addParameter(requestTypeName, "request")
            .addModifiers(PUBLIC, DEFAULT)
            .addStatement("return $L($T.identityHashCode(request), request)", methodName, System.class)
            .build();
    }

    private MethodSpec makeRequestWithIdMethod(
        String methodName, TypeName descTypeName, TypeName requestTypeName, TypeName responseTypeName
    ) {

        return MethodSpec.methodBuilder(methodName)
            .returns(responseTypeName)
            .addParameters(Arrays.asList(
                ParameterSpec.builder(TypeName.LONG, "requestId").build(),
                ParameterSpec.builder(requestTypeName, "request").build())
            ).addModifiers(PUBLIC, DEFAULT)
            .addStatement(
                "return getCaller().call($L, requestId, request, $LHandlers)",
                methodName, methodName
            ).build();
    }

    private FieldSpec addHandlers(String method, TypeName req, TypeName res) {
        return FieldSpec
            .builder(ParameterizedTypeName.get(SERVICE_CALL_CYCLE, req, res), method + "Handlers")
            .addModifiers(PUBLIC, FINAL, STATIC)
            .initializer(
                "new $T<>($L, $L.$L)", SERVICE_CALL_CYCLE, method, LOWER_CAMEL.to(UPPER_CAMEL, method), "logger"
            )
            .build();
    }

    private TypeSpec addHandlerLogClass(ClassName serviceClass, String method) {
        String methodClassName = LOWER_CAMEL.to(UPPER_CAMEL, method);
        return TypeSpec.classBuilder(serviceClass.nestedClass(methodClassName))
            .addModifiers(PUBLIC, FINAL, STATIC)
            .addField(FieldSpec
                .builder(Logger.class, "logger", PUBLIC, FINAL, STATIC)
                .initializer("$T.getLogger($L.class)", LoggerFactory.class, methodClassName)
                .build())
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

    private void createJavaFile(String packageName, TypeSpec typeSpec, ClassName... staticImport) throws IOException {
        JavaFile.Builder builder = JavaFile.builder(packageName, typeSpec);
        for (ClassName className : staticImport) {
            builder.addStaticImport(className, "*");
        }
        JavaFile javaFile = builder
            .indent("    ")
            .skipJavaLangImports(true)
            .build();
        javaFile.writeTo(processingEnv.getFiler());

    }

}
