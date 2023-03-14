/*
 * Copyright 2021, Zetyun DataPortal All rights reserved.
 */

package io.dingodb.sdk.common.partition;

import java.util.List;

public class PartitionDetailDefinition implements PartitionDetail {

    private String partName;
    private String operator;
    private List<Object> operand;

    public PartitionDetailDefinition(Object partName, String operator, List<Object> operand) {
        if (partName != null) {
            this.partName = partName.toString();
        }
        this.operator = operator;
        this.operand = operand;
    }

    @Override
    public String getPartName() {
        return partName;
    }

    @Override
    public String getOperator() {
        return operator;
    }

    @Override
    public List<Object> getOperand() {
        return operand;
    }
}
