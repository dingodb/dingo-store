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

package io.dingodb.sdk.common.serial.schema;

import io.dingodb.sdk.common.serial.Buf;

public interface DingoSchema<T> {

    byte NULL = 0;
    byte NOTNULL = 1;

    Type getType();

    void setIndex(int index);

    int getIndex();

    void setIsKey(boolean isKey);

    boolean isKey();

    int getLength();

    void setAllowNull(boolean allowNull);

    boolean isAllowNull();

    void encodeKey(Buf buf, T data);
    void encodeKeyForUpdate(Buf buf, T data);
    T decodeKey(Buf buf);
    T decodeKeyPrefix(Buf buf);
    void skipKey(Buf buf);

    void encodeKeyPrefix(Buf buf, T data);

    void encodeValue(Buf buf, T data);
    T decodeValue(Buf buf);
    void skipValue(Buf buf);

}
