/*
 * Copyright 2021, Zetyun DataPortal All rights reserved.
 */

package io.dingodb.sdk.common;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public class RangeWithOptions {

    private Range range;
    private boolean withStart;
    private boolean withEnd;
}
