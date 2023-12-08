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

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;

@Getter
@ToString
@EqualsAndHashCode
@NoArgsConstructor
public class VectorIndexParameter {

    private VectorIndexType vectorIndexType;
    private FlatParam flatParam;
    private IvfFlatParam ivfFlatParam;
    private IvfPqParam ivfPqParam;
    private HnswParam hnswParam;
    private DiskAnnParam diskAnnParam;
    private BruteForceParam bruteForceParam;

    public VectorIndexParameter(VectorIndexType vectorIndexType, FlatParam flatParam) {
        this.vectorIndexType = vectorIndexType;
        this.flatParam = flatParam;
    }

    public VectorIndexParameter(VectorIndexType vectorIndexType, IvfFlatParam ivfFlatParam) {
        this.vectorIndexType = vectorIndexType;
        this.ivfFlatParam = ivfFlatParam;
    }

    public VectorIndexParameter(VectorIndexType vectorIndexType, IvfPqParam ivfPqParam) {
        this.vectorIndexType = vectorIndexType;
        this.ivfPqParam = ivfPqParam;
    }

    public VectorIndexParameter(VectorIndexType vectorIndexType, HnswParam hnswParam) {
        this.vectorIndexType = vectorIndexType;
        this.hnswParam = hnswParam;
    }

    public VectorIndexParameter(VectorIndexType vectorIndexType, DiskAnnParam diskAnnParam) {
        this.vectorIndexType = vectorIndexType;
        this.diskAnnParam = diskAnnParam;
    }

    public VectorIndexParameter(VectorIndexType vectorIndexType, BruteForceParam bruteForceParam) {
        this.vectorIndexType = vectorIndexType;
        this.bruteForceParam = bruteForceParam;
    }

    public enum VectorIndexType {
        VECTOR_INDEX_TYPE_NONE,
        VECTOR_INDEX_TYPE_FLAT,
        VECTOR_INDEX_TYPE_IVF_FLAT,
        VECTOR_INDEX_TYPE_IVF_PQ,
        VECTOR_INDEX_TYPE_HNSW,
        VECTOR_INDEX_TYPE_DISKANN,
        VECTOR_INDEX_TYPE_BRUTEFORCE
    }

    public enum MetricType {
        METRIC_TYPE_NONE,  // this is a placeholder
        METRIC_TYPE_L2,
        METRIC_TYPE_INNER_PRODUCT,
        METRIC_TYPE_COSINE
    }
}
