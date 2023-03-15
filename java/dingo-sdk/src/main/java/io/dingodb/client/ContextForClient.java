/*
 * Copyright 2021, Zetyun DataPortal All rights reserved.
 */

package io.dingodb.client;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;

import java.util.List;

@Builder
@Getter
@AllArgsConstructor
public class ContextForClient {
    private final List<Object[]> keyList;
    private final List<Object[]> values;
}
