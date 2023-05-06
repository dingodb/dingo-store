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

import java.util.Arrays;
import java.util.List;

import static io.dingodb.error.ErrorOuterClass.Errno.EKEY_OUT_OF_RANGE_VALUE;
import static io.dingodb.error.ErrorOuterClass.Errno.EREGION_REDIRECT_VALUE;

public class ErrorCodeUtils {

    public static final List<Integer> refreshCode = Arrays.asList(
            EKEY_OUT_OF_RANGE_VALUE,
            EREGION_REDIRECT_VALUE);

    public static final List<Integer> retryCode = Arrays.asList();
}
