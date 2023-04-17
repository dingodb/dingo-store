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

package io.dingodb.client;

import io.dingodb.sdk.common.DingoCommonId;
import io.dingodb.sdk.common.KeyValue;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;

import java.util.Arrays;
import java.util.List;

@Builder
@Getter
@AllArgsConstructor
public class ContextForStore {

    private final List<byte[]> startKeyInBytes;
    private final List<KeyValue> recordList;
    private final DingoCommonId regionId;

    public KeyValue getRecordByKey(byte[] key) {
        if (recordList == null) {
            return null;
        }

        KeyValue result = null;
        for (KeyValue record : recordList) {
            if (Arrays.equals(record.getKey(), key)) {
                result = record;
                break;
            }
        }
        return result;
    }
}
