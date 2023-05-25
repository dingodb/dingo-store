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

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

import java.util.List;

@Getter
@ToString
@EqualsAndHashCode
public class PartitionDetailDefinition implements PartitionDetail {

    private String partName;
    private String operator;
    private Object[] operand;

    public PartitionDetailDefinition(String partName, String operator, List<Object> operand) {
        this(partName, operator, operand.toArray());
    }

    public PartitionDetailDefinition(Object partName, String operator, Object[] operand) {
        if (partName != null) {
            this.partName = partName.toString();
        }
        this.operator = operator;
        this.operand = operand;
    }

}
