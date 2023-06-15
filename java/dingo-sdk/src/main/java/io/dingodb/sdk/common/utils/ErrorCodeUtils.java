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
import java.util.function.Function;

import static io.dingodb.error.ErrorOuterClass.Errno.*;

public class ErrorCodeUtils {

    public enum InternalCode {
        FAILED,
        RETRY,
        REFRESH,
        IGNORE,
        ;
    }

    public static final List<Integer> refreshCode = Arrays.asList(
        EKEY_OUT_OF_RANGE_VALUE,
        EREGION_REDIRECT_VALUE,
        EREGION_SPLITING_VALUE,
        EREGION_MERGEING_VALUE
    );

    public static final List<Integer> retryCode = Arrays.asList(
        ERAFT_INIT_VALUE,
        ERAFT_NOTLEADER_VALUE,
        ERAFT_COMMITLOG_VALUE,
        EREGION_UNAVAILABLE_VALUE,
        EREGION_PEER_CHANGEING_VALUE,
        EREGION_STATE_VALUE
    );

    public static final List<Integer> ignoreCode = Arrays.asList(
        ETABLE_NOT_FOUND_VALUE,
        ESCHEMA_NOT_FOUND_VALUE
    );

    public static final Function<Integer, InternalCode> defaultCodeChecker = code -> {
        if (ignoreCode.contains(code)) {
            return InternalCode.IGNORE;
        }
        if (refreshCode.contains(code)) {
            return InternalCode.REFRESH;
        }
        if (retryCode.contains(code)) {
            return InternalCode.RETRY;
        }
        return InternalCode.FAILED;
    };
}
