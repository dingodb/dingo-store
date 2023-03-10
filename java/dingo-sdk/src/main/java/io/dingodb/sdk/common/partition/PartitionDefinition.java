/*
 * Copyright 2021, Zetyun DataPortal All rights reserved.
 */

package io.dingodb.sdk.common.partition;

import java.util.List;

public class PartitionDefinition implements Partition {

    String funcName;

    List<String> cols;

    List<PartitionDetailDefinition> details;

    public PartitionDefinition(String funcName, List<String> cols, List<PartitionDetailDefinition> details) {
        this.funcName = funcName;
        this.cols = cols;
        this.details = details;
    }

    @Override
    public String funcName() {
        return null;
    }

    @Override
    public List<String> cols() {
        return null;
    }

    @Override
    public List<PartitionDetail> details() {
        return null;
    }
}
