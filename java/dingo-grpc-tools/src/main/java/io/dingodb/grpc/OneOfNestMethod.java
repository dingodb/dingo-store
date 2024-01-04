package io.dingodb.grpc;

import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.TypeName;
import com.squareup.javapoet.TypeSpec;

import static javax.lang.model.element.Modifier.ABSTRACT;
import static javax.lang.model.element.Modifier.PUBLIC;

public class OneOfNestMethod {

    public static void addNestMethod(ClassName nestName, TypeSpec.Builder nestBuilder) {
        if (nestName.simpleName().equals("VectorIndexParameterNest")) {
            nestBuilder.addMethod(MethodSpec
                .methodBuilder("getDimension")
                .addModifiers(PUBLIC, ABSTRACT)
                .returns(TypeName.INT)
                .build()
            ).addMethod(MethodSpec
                .methodBuilder("getMetricType")
                .addModifiers(PUBLIC, ABSTRACT)
                .returns(ClassName.bestGuess("io.dingodb.sdk.service.entity.common.MetricType"))
                .build()
            );
        }

    }

}
