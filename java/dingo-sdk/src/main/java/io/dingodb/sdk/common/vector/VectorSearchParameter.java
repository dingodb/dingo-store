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

package io.dingodb.sdk.common.vector;

import io.dingodb.sdk.service.store.Coprocessor;
import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.List;

@Getter
@AllArgsConstructor
public class VectorSearchParameter {

    private Integer topN;
    private boolean withoutVectorData;
    private boolean withoutScalarData;
    private List<String> selectedKeys;
    private boolean withoutTableData;
    private Search search;

    private VectorFilter vectorFilter;
    private VectorFilterType vectorFilterType;
    private Coprocessor coprocessor;
    private List<Long> vectorIds;

    private boolean useBruteForce;

    @Deprecated
    public VectorSearchParameter(
            Integer topN,
            boolean withoutVectorData,
            boolean withoutScalarData,
            List<String> selectedKeys,
            boolean withoutTableData,
            Search search,
            VectorFilter vectorFilter,
            VectorFilterType vectorFilterType,
            Coprocessor coprocessor,
            List<Long> vectorIds
    ) {
        this(topN, withoutVectorData, withoutScalarData, selectedKeys, withoutTableData, search,
                vectorFilter, vectorFilterType, coprocessor, vectorIds, false);
    }

    public enum VectorFilter {
        SCALAR_FILTER,
        TABLE_FILTER,
        VECTOR_ID_FILTER
    }

    public enum VectorFilterType {
        // first vector search, then filter
        QUERY_POST,
        // first search from rocksdb, then search vector
        QUERY_PRE
    }
}
