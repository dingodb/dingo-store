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

import io.dingodb.common.Common;
import io.dingodb.sdk.common.index.IndexParameter;
import io.dingodb.sdk.common.partition.Partition;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public interface Table {

    String getName();

    List<Column> getColumns();

    int getVersion();

    int getTtl();

    Partition getPartition();

    String getEngine();

    Map<String, String> getProperties();

    int getReplica();

    long getAutoIncrement();

    String getCreateSql();

    IndexParameter getIndexParameter();

    String getComment();

    String getCharset();

    String getCollate();

    default String getTableType() {
        return "BASE TABLE";
    }

    default String getRowFormat() {
        return "Dynamic";
    }

    default long getCreateTime() {
        return System.currentTimeMillis();
    }

    default long getUpdateTime() {
        return 0;
    }

    default int getPrimaryKeyCount() {
        int count = 0;
        for (Column column : getColumns()) {
            if (column.isPrimary()) {
                count++;
            }
        }
        return count;
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

    default int getColumnIndex(String name) {
        int i = 0;
        for (Column column : getColumns()) {
            // `name` may be uppercase.
            if (column.getName().equalsIgnoreCase(name)) {
                return i;
            }
            ++i;
        }
        return -1;
    }

    default List<Integer> getKeyColumnIndices() {
        return getColumnIndices(true);
    }

    default List<Column> getKeyColumns() {
        List<Column> keyCols = new LinkedList<>();
        for (Column column : getColumns()) {
            if (column.isPrimary()) {
                keyCols.add(column);
            }
        }
        return keyCols;
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
            for (Integer integer : indices) {
                pkIndices[getColumns().get(integer).getPrimary()] = integer;
            }
            return Arrays.asList(pkIndices);
        }
        return indices;
    }

}
