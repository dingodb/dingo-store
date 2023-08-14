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

package io.dingodb.sdk.service.store;

import io.dingodb.sdk.common.table.Column;

import java.util.List;

public interface Coprocessor {

    int getSchemaVersion();

    SchemaWrapper getOriginalSchema();

    default SchemaWrapper getResultSchema() {
        throw new UnsupportedOperationException();
    }

    List<Integer> getSelection();

    byte[] getExpression();

    default List<Integer> getGroupBy() {
        throw new UnsupportedOperationException();
    }

    default List<AggregationOperator> getAggregations() {
        throw new UnsupportedOperationException();
    }

    interface SchemaWrapper {
        List<Column> getSchemas();

        long getCommonId();
    }
}
