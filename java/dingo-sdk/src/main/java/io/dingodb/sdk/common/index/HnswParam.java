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
import lombok.*;

@Getter
@ToString
@EqualsAndHashCode
@NoArgsConstructor
@AllArgsConstructor
public class HnswParam implements VectorIndexParam {

    private Integer dimension;
    private VectorIndexParameter.MetricType metricType;
    private Integer efConstruction;
    @Setter
    private Integer maxElements;
    private Integer nlinks;

    @Override
    public <T> T toProto() {
        return (T) Common.CreateHnswParam.newBuilder()
            .setDimension(dimension)
            .setMetricType(Common.MetricType.valueOf(metricType.name()))
            .setEfConstruction(efConstruction)
            .setMaxElements(maxElements)
            .setNlinks(nlinks)
            .build();
    }

}
