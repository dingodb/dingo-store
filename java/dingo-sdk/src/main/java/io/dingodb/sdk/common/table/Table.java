/*
 * Copyright 2021, Zetyun DataPortal All rights reserved.
 */

package io.dingodb.sdk.common.table;

import io.dingodb.sdk.common.partition.Partition;
import io.dingodb.sdk.common.type.DingoType;
import io.dingodb.sdk.common.type.DingoTypeFactory;
import io.dingodb.sdk.common.type.TupleMapping;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public interface Table {

    void setName(String name);
    String getName();

    void setColumnDefinitions(List<Column> columns);
    List<Column> getColumns();

    void setVersion(int version);
    int getVersion();

    void setTtl(int ttl);
    int getTtl();

    void setPartitionDefinition(Partition partDefinition);
    Partition getPartDefinition();

    void setEngine(String engine);
    String getEngine();

    void setProperties(Map<String, String> properties);
    Map<String, String> getProperties();

    default int getPrimaryKeyCount() {
        int count = 0;
        for (Column column : getColumns()) {
            if (column.isPrimary()) {
                count++;
            }
        }
        return count;
    }

    default DingoType getDingoType() {
        return DingoTypeFactory.tuple(
            getColumns().stream()
                .map(Column::getDingoType)
                .toArray(DingoType[]::new)
        );
    }

    default TupleMapping getKeyMapping() {
        return TupleMapping.of(getKeyColumnIndices());
    }

    default List<Integer> getKeyColumnIndices() {
        return getColumnIndices(true);
    }

    default List<Integer> getColumnIndices(boolean keyOrValue) {
        List<Integer> indices = new LinkedList<>();
        int index = 0;
        for (Column column : getColumns()) {
            if (column.isPrimary() == keyOrValue) {
                indices.add(index);
            }
            ++index;
        }
        if (keyOrValue) {
            Integer[] pkIndices = new Integer[indices.size()];
            for (int i = 0; i < indices.size(); i++) {
                pkIndices[getColumns().get(indices.get(i)).getPrimary()] = indices.get(i);
            }
            return Arrays.asList(pkIndices);
        }
        return indices;
    }
}
