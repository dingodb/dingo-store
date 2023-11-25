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

package io.dingodb.sdk.common.utils;

import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Collection;
import java.util.Objects;
import java.util.function.Predicate;
import java.util.function.Supplier;

@Slf4j
public final class Parameters {

    private Parameters() {
    }

    /**
     * If input is null, throw {@link NullPointerException}, else return input.
     *
     * @param <T> input type
     * @param input check object
     * @param message exception message if input is null
     * @return input
     */
    public static <T> @NonNull T nonNull(T input, String message) {
        return check(input, Objects::nonNull, () -> new NullPointerException(message));
    }

    /**
     * If input is null or empty, throw {@link IllegalArgumentException}, else return input.
     *
     * @param <C> input collection type
     * @param input check object
     * @param message exception message if input is null
     * @return input
     */
    public static <E, C extends Collection<E>> @NonNull C notEmpty(C input, String message) {
        return check(input, in -> in != null && !in.isEmpty(), () -> new IllegalArgumentException(message));
    }

    /**
     * If check function return is false, throw exception, else return input.
     * @param <T> input type
     * @param input check object
     * @param checkFunction check function
     * @param message error message
     * @return input
     */
    public static <T> T check(
        T input,
        Predicate<T> checkFunction,
        String message
    ) {
        return check(input, checkFunction, () -> new IllegalArgumentException(message));
    }

    /**
     * If check function return is false, throw exception, else return input.
     * @param <T> input type
     * @param input check object
     * @param checkFunction check function
     * @param throwableSupplier throwable supplier
     * @return input
     */
    public static <T> T check(
            T input,
            Predicate<T> checkFunction,
            Supplier<RuntimeException> throwableSupplier
    ) {
        Exception testEx = null;
        try {
            if (checkFunction.test(input)) {
                return input;
            }
        } catch (Exception e) {
            log.error(
                    "Run check function error, input is: --[{}]--, caller is [{}].",
                    input,
                    Thread.currentThread().getStackTrace()[2].getMethodName(),
                    e
            );
            testEx = e;
        }
        RuntimeException exception = throwableSupplier.get();
        if (exception == null) {
            if (testEx == null) {
                log.warn(
                        "Run check function error, input is: --[{}]--, caller is [{}].",
                        input,
                        Thread.currentThread().getStackTrace()[2].getMethodName()
                );
            } else {
                log.warn(
                        "Run check function error, but it is ignore, input is: --[{}]--, caller is [{}].",
                        input,
                        Thread.currentThread().getStackTrace()[2].getMethodName()
                );
            }
            return input;
        }
        throw exception;
    }

    /**
     * If check function return is false, return default value, else return input.
     * @param <T> input type
     * @param input check object
     * @param defaultValue default value
     * @return input
     */
    public static <T> T cleanNull(T input, T defaultValue) {
        return clean(input, Objects::nonNull, defaultValue);
    }

    /**
     * If check function return is false, return default value, else return input.
     * @param <T> input type
     * @param input check object
     * @param valueSupplier default value supplier
     * @return input
     */
    public static <T> T cleanNull(T input, Supplier<T> valueSupplier) {
        return clean(input, (Predicate<T>) Objects::nonNull, valueSupplier);
    }

    /**
     * If check function return is false, return default value, else return input.
     * @param <T> input type
     * @param input check object
     * @param checkFunction check function
     * @param valueSupplier default value supplier
     * @return input
     */
    public static <T> T clean(
            T input,
            Predicate<T> checkFunction,
            Supplier<T> valueSupplier
    ) {
        try {
            if (checkFunction.test(input)) {
                return input;
            }
        } catch (Exception ignored) {
        }
        return valueSupplier.get();
    }

    /**
     * If check function return is false, return default value, else return input.
     * @param <T> input type
     * @param input check object
     * @param checkFunction check function
     * @param defaultValue default value supplier
     * @return input
     */
    public static <T> T clean(
            T input,
            Predicate<T> checkFunction,
            T defaultValue
    ) {
        try {
            if (checkFunction.test(input)) {
                return input;
            }
        } catch (Exception ignored) {
        }
        return defaultValue;
    }
}
