/*
 * Copyright 2021, Zetyun DataPortal All rights reserved.
 */

package io.dingodb.client;

import io.dingodb.common.Common;
import lombok.Getter;

import java.util.List;

@Getter
public class ResultForStore {
    private final int code;
    private final String errorMessage;
    private final List<Common.KeyValue> records;

    public ResultForStore(int code, String errorMessage) {
        this(code, errorMessage, null);
    }

    public ResultForStore(int code, String errorMessage, List<Common.KeyValue> records) {
        this.code = code;
        this.errorMessage = errorMessage;
        this.records = records;
    }
}
