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
public class TableDefinition implements Table {

    private String name;
    private List<Column> columns;
    private int version;
    private int ttl;
    private Partition partition;
    private String engine;
    private Map<String, String> properties;
    private int replica;
    private long autoIncrement = 1;

    @Deprecated
    public TableDefinition(String name, List<Column> columns, int version, int ttl, Partition partition, String engine, Map<String, String> properties) {
        this(name, columns, version, ttl, partition, engine, properties, 3);
    }

    @Deprecated
    public TableDefinition(String name, List<Column> columns, int version, int ttl, Partition partition, String engine, Map<String, String> properties, int replica) {
        this(name, columns, version, ttl, partition, engine, properties, replica, 1);
    }

    @Deprecated
    public TableDefinition(String name, List<Column> columns, int version, int ttl, Partition partition, String engine, Map<String, String> properties, int replica, long autoIncrement) {
        this.name = name;
        this.columns = columns;
        this.version = version;
        this.ttl = ttl;
        this.partition = partition;
        this.engine = engine;
        this.properties = properties;
        this.replica = replica;
        this.autoIncrement = autoIncrement;
    }

    @Override
    public String getName() {
        return this.name;
    }

    @Override
    public List<Column> getColumns() {
        return columns;
    }

    @Override
    public int getVersion() {
        return this.version;
    }

    @Override
    public int getTtl() {
        return this.ttl;
    }

    @Override
    public Partition getPartDefinition() {
        return this.partition;
    }

    @Override
    public String getEngine() {
        return this.engine;
    }

    @Override
    public Map<String, String> getProperties() {
        return this.properties;
    }

    @Override
    public int getReplica() {
        return replica;
    }

    @Override
    public long autoIncrement() {
        return autoIncrement;
    }
}
