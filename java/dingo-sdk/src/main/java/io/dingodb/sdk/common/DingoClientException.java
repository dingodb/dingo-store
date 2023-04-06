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

package io.dingodb.sdk.common;

import io.dingodb.error.ErrorOuterClass;

public class DingoClientException extends IllegalArgumentException {

    protected int errorCode;

    public DingoClientException(int errorCode, String message) {
        super(message);
        this.errorCode = errorCode;
    }

    public DingoClientException(int errorCode, Throwable exception) {
        super(exception);
        this.errorCode = errorCode;
    }

    public DingoClientException(String message) {
        super(message);
    }

    public DingoClientException(int errorCode) {
        super();
        this.errorCode = errorCode;
    }

    public DingoClientException(int errorCode, String message, Throwable ex) {
        super(message, ex);
        this.errorCode = errorCode;
    }

    public static final class InvalidStoreLeader extends DingoClientException {

        public InvalidStoreLeader(String tableName) {
            super(ErrorOuterClass.Errno.ERAFT_NOTLEADER_VALUE,
                    "Invalid store leader, the store leader is empty, and the table name is: " + tableName);
        }
    }
}
