package io.dingodb.sdk.common.utils;

import java.lang.reflect.ParameterizedType;

public final class ReflectionUtils {

    private ReflectionUtils() {
    }

    public static Class getGenericType(Class<?> cls, int n) {
        return (Class) ((ParameterizedType)cls.getGenericSuperclass()).getActualTypeArguments()[n];
    }

}
