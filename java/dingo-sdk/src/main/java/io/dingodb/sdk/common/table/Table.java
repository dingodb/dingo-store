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

import io.dingodb.meta.Meta;
import io.dingodb.sdk.common.codec.DingoKeyValueCodec;
import io.dingodb.sdk.common.codec.KeyValueCodec;
import io.dingodb.sdk.common.partition.Partition;
import io.dingodb.sdk.common.type.DingoType;
import io.dingodb.sdk.common.type.DingoTypeFactory;
import io.dingodb.sdk.common.type.TupleMapping;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public interface Table {

    String getName();

    List<Column> getColumns();

    int getVersion();

    int getTtl();

    Partition getPartDefinition();

    String getEngine();

    Map<String, String> getProperties();

    int getReplica();

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

    default Column getColumn(int index) {
        return getColumns().get(index);
    }

    default Column getColumn(String name) {
        for (Column column : getColumns()) {
            if (column.getName().equalsIgnoreCase(name)) {
                return column;
            }
        }
        return null;
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

    default KeyValueCodec createCodec(Meta.DingoCommonId tableId) {
        return new DingoKeyValueCodec(getDingoType(), getKeyMapping(), tableId.getEntityId());
    }
}
