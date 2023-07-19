package io.dingodb.sdk.common.vector;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.util.List;

@Getter
@NoArgsConstructor
@AllArgsConstructor
public class VectorWithDistanceResult {

    private List<VectorWithDistance> withDistance;
}
