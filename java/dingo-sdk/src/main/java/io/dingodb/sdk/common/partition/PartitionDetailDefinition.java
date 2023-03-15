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
