package io.dingodb.sdk.common.index;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;

@Getter
@ToString
@EqualsAndHashCode
@NoArgsConstructor
@AllArgsConstructor
public class BruteForceParam {

    private Integer dimension;
    private VectorIndexParameter.MetricType metricType;
}
