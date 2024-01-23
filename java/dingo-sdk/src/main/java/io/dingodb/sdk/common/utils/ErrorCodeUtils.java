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

import static io.dingodb.error.ErrorOuterClass.Errno.EKEY_OUT_OF_RANGE_VALUE;
import static io.dingodb.error.ErrorOuterClass.Errno.ELEASE_NOT_EXISTS_OR_EXPIRED_VALUE;
import static io.dingodb.error.ErrorOuterClass.Errno.ERAFT_COMMITLOG_VALUE;
import static io.dingodb.error.ErrorOuterClass.Errno.ERAFT_INIT_VALUE;
import static io.dingodb.error.ErrorOuterClass.Errno.ERAFT_NOTLEADER_VALUE;
import static io.dingodb.error.ErrorOuterClass.Errno.EREGION_MERGEING_VALUE;
import static io.dingodb.error.ErrorOuterClass.Errno.EREGION_NOT_FOUND_VALUE;
import static io.dingodb.error.ErrorOuterClass.Errno.EREGION_PEER_CHANGEING_VALUE;
import static io.dingodb.error.ErrorOuterClass.Errno.EREGION_REDIRECT_VALUE;
import static io.dingodb.error.ErrorOuterClass.Errno.EREGION_SPLITING_VALUE;
import static io.dingodb.error.ErrorOuterClass.Errno.EREGION_STATE_VALUE;
import static io.dingodb.error.ErrorOuterClass.Errno.EREGION_UNAVAILABLE_VALUE;
import static io.dingodb.error.ErrorOuterClass.Errno.EREGION_VERSION_VALUE;
import static io.dingodb.error.ErrorOuterClass.Errno.EREQUEST_FULL_VALUE;
import static io.dingodb.error.ErrorOuterClass.Errno.ESCHEMA_NOT_FOUND_VALUE;
import static io.dingodb.error.ErrorOuterClass.Errno.ETABLE_NOT_FOUND_VALUE;
import static io.dingodb.error.ErrorOuterClass.Errno.EVECTOR_INDEX_NOT_READY_VALUE;

public final class ErrorCodeUtils {

    private ErrorCodeUtils() {
    }

    public enum Strategy {
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
        EREGION_MERGEING_VALUE,
        EREGION_NOT_FOUND_VALUE,
        EREGION_VERSION_VALUE
    );

    public static final List<Integer> retryCode = Arrays.asList(
        ERAFT_INIT_VALUE,
        ERAFT_NOTLEADER_VALUE,
        ERAFT_COMMITLOG_VALUE,
        EREGION_UNAVAILABLE_VALUE,
        EREGION_PEER_CHANGEING_VALUE,
        EREGION_STATE_VALUE,
        EVECTOR_INDEX_NOT_READY_VALUE,
        EREQUEST_FULL_VALUE,
        ELEASE_NOT_EXISTS_OR_EXPIRED_VALUE
    );

    public static final List<Integer> ignoreCode = Arrays.asList(
        ETABLE_NOT_FOUND_VALUE,
        ESCHEMA_NOT_FOUND_VALUE
    );

    public static Strategy errorToStrategy(int code) {
        if (ignoreCode.contains(code)) {
            return Strategy.IGNORE;
        }
        if (refreshCode.contains(code)) {
            return Strategy.REFRESH;
        }
        if (retryCode.contains(code)) {
            return Strategy.RETRY;
        }
        return Strategy.FAILED;
    }

    public static final Function<Integer, Strategy> errorToStrategyFunc = ErrorCodeUtils::errorToStrategy;
}
