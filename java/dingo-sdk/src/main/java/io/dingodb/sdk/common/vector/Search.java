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

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;

@Getter
@ToString
@NoArgsConstructor
public class Search {

    private SearchFlatParam flat;
    private SearchIvfFlatParam ivfFlatParam;
    private SearchIvfPqParam ivfPqParam;
    private SearchHnswParam hnswParam;
    private SearchDiskAnnParam diskAnnParam;

    public Search(SearchFlatParam flat) {
        this.flat = flat;
    }

    public Search(SearchIvfFlatParam ivfFlatParam) {
        this.ivfFlatParam = ivfFlatParam;
    }

    public Search(SearchIvfPqParam ivfPqParam) {
        this.ivfPqParam = ivfPqParam;
    }

    public Search(SearchHnswParam hnswParam) {
        this.hnswParam = hnswParam;
    }

    public Search(SearchDiskAnnParam diskAnnParam) {
        this.diskAnnParam = diskAnnParam;
    }
}
