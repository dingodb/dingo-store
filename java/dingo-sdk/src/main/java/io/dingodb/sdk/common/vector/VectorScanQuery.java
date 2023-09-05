package io.dingodb.sdk.common.vector;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.util.List;
import java.util.Map;

@Getter
@NoArgsConstructor
@AllArgsConstructor
public class VectorScanQuery {

    private Long startId;
    private Boolean isReverseScan;
    private Long maxScanCount;
    private Long endId;

    private Boolean withoutVectorData;
    private Boolean withoutScalarData;
    private List<String> selectedKeys;
    private Boolean withoutTableData;

    // Whether to use scalar filtering.
    private Boolean useScalarFilter;
    private Map<String, ScalarValue> scalarForFilter;

}
