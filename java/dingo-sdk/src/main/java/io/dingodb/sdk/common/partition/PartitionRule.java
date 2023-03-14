/*
 * Copyright 2021, Zetyun DataPortal All rights reserved.
 */

package io.dingodb.sdk.common.partition;

import java.util.List;

public class PartitionRule implements Partition {

    String strategy;

    List<String> cols;

    List<PartitionDetailDefinition> details;

    public PartitionRule(String strategy, List<String> cols, List<PartitionDetailDefinition> details) {
        this.strategy = strategy;
        this.cols = cols;
        this.details = details;
    }

    @Override
    public String strategy() {
        return strategy;
    }

    @Override
    public List<String> cols() {
        return cols;
    }

    @Override
    public List<PartitionDetailDefinition> details() {
        return details;
    }
}
