/*
 * Copyright 2021, Zetyun DataPortal All rights reserved.
 */

package io.dingodb.client;

import lombok.Getter;

import java.util.List;

@Getter
public class Result {

    private final boolean isSuccess;
    private final String errorMessage;
    private final List<String> keyColumns;
    private final List<Object[]> values;

    public Result(boolean isSuccess, String errorMessage) {
        this(isSuccess, errorMessage, null, null);
    }

    public Result(boolean isSuccess, String errorMessage, List<String> keyColumns, List<Object[]> columns) {
        this.isSuccess = isSuccess;
        this.errorMessage = errorMessage;
        this.keyColumns = keyColumns;
        this.values = columns;
    }
}
