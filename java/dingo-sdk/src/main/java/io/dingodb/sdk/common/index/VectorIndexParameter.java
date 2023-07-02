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

import io.dingodb.common.Common;
import io.dingodb.common.Common.VectorIndexType;
import lombok.Getter;
import lombok.ToString;

@Getter
@ToString
public class VectorIndexParameter extends AbstractIndexParameter {

    private Integer dimension;
    private Common.MetricType metricType;
    private Integer efConstruction;
    private Integer maxElements;
    private Integer nlinks;

    public VectorIndexParameter(
            IndexType indexType,
            Integer dimension,
            Common.MetricType metricType,
            Integer efConstruction,
            Integer maxElements,
            Integer nlinks) {
        super(indexType);
        this.dimension = dimension;
        this.metricType = metricType;
        this.efConstruction = efConstruction;
        this.maxElements = maxElements;
        this.nlinks = nlinks;
    }

    public enum VectorIndexType implements IndexType {
        VECTOR_INDEX_TYPE_NONE,
        VECTOR_INDEX_TYPE_FLAT,
        VECTOR_INDEX_TYPE_IVF_FLAT,
        VECTOR_INDEX_TYPE_IVF_PQ,
        VECTOR_INDEX_TYPE_HNSW,
        VECTOR_INDEX_TYPE_DISKANN
    }
}
