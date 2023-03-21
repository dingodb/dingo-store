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

import io.dingodb.sdk.common.partition.Partition;
import lombok.AllArgsConstructor;
import lombok.Builder;

import java.util.List;
import java.util.Map;

@Builder
@AllArgsConstructor
public class TableDefinition implements Table {

    private String name;
    private List<Column> columns;
    private int version;
    private int ttl;
    private Partition partition;
    private String engine;
    private Map<String, String> properties;

    @Override
    public void setName(String name) {
        this.name = name;
    }

    @Override
    public String getName() {
        return this.name;
    }

    @Override
    public void setColumnDefinitions(List<Column> columns) {
        this.columns = columns;
    }

    @Override
    public List<Column> getColumns() {
        return columns;
    }

    @Override
    public void setVersion(int version) {
        this.version = version;
    }

    @Override
    public int getVersion() {
        return this.version;
    }

    @Override
    public void setTtl(int ttl) {
        this.ttl = ttl;
    }

    @Override
    public int getTtl() {
        return this.ttl;
    }

    @Override
    public void setPartitionDefinition(Partition partDefinition) {
        this.partition = partDefinition;
    }

    @Override
    public Partition getPartDefinition() {
        return this.partition;
    }

    @Override
    public void setEngine(String engine) {
        this.engine = engine;
    }

    @Override
    public String getEngine() {
        return this.engine;
    }

    @Override
    public void setProperties(Map<String, String> properties) {
        this.properties = properties;
    }

    @Override
    public Map<String, String> getProperties() {
        return this.properties;
    }

}
