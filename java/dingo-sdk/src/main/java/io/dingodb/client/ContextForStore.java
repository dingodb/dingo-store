/*
 * Copyright 2021, Zetyun DataPortal All rights reserved.
 */

package io.dingodb.client;

import io.dingodb.meta.Meta;
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
    private final Meta.DingoCommonId regionId;

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
