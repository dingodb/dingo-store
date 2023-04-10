/*
 * Copyright 2021, Zetyun DataPortal All rights reserved.
 */

package io.dingodb.sdk.common.table.metric;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public class TableMetrics {

    private byte[] minKey;
    private byte[] maxKey;
    private long rowCount;
    private long partCount;
}
