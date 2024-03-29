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

package io.dingodb.sdk.common.index;

import io.dingodb.sdk.common.partition.Partition;
import io.dingodb.sdk.common.partition.PartitionRule;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.NoArgsConstructor;
import lombok.ToString;

@Builder
@ToString
@NoArgsConstructor
@AllArgsConstructor
public class IndexDefinition implements Index {

    private String name;
    private Integer version;
    private PartitionRule partitionRule;
    private Integer replica;
    private IndexParameter parameter;
    private Boolean isAutoIncrement;
    private Long autoIncrement;

    @Override
    public String getName() {
        return name;
    }

    @Override
    public Integer getVersion() {
        return version;
    }

    @Override
    public Partition getIndexPartition() {
        return partitionRule;
    }

    @Override
    public Integer getReplica() {
        return replica;
    }

    @Override
    public IndexParameter getIndexParameter() {
        return parameter;
    }

    @Override
    public Boolean getIsAutoIncrement() {
        return isAutoIncrement;
    }

    @Override
    public Long getAutoIncrement() {
        return autoIncrement;
    }
}
