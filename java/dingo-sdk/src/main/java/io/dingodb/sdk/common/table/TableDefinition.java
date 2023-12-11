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

package io.dingodb.sdk.common.table;

import io.dingodb.sdk.common.index.IndexParameter;
import io.dingodb.sdk.common.partition.Partition;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.util.List;
import java.util.Map;

@Getter
@Builder
@ToString
@EqualsAndHashCode
@AllArgsConstructor
public class TableDefinition implements Table {

    private String name;
    private List<Column> columns;
    private int version;
    private int ttl;
    private Partition partition;
    private String engine;
    private Map<String, String> properties;
    private int replica;
    @Builder.Default
    private long autoIncrement = 1;
    private String createSql;
    private IndexParameter indexParameter;
    private String comment;
    private String charset;
    private String collate;
    private String tableType;
    private String rowFormat;
    private long createTime;
    private long updateTime;

}
