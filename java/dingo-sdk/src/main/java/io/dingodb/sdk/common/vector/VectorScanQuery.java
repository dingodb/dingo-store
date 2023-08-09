package io.dingodb.sdk.common.vector;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Getter
@NoArgsConstructor
@AllArgsConstructor
public class VectorScanQuery {

    private Long startId;
    private Boolean isReverseScan;
    private Long maxScanCount;
    private Boolean withoutVectorData;
    private Boolean withScalarData;
    private List<String> selectedKeys;
    private Boolean withTableData;

    // Whether to use scalar filtering.
    private Boolean useScalarFilter;
    private Map<String, ScalarValue> scalarForFilter;

    @Deprecated
    public VectorScanQuery(
            Long startId,
            Boolean isReverseScan,
            Long maxScanCount,
            Boolean withoutVectorData,
            Boolean withScalarData,
            List<String> selectedKeys,
            Boolean withTableData
    ) {
        this.startId = startId;
        this.isReverseScan = isReverseScan;
        this.maxScanCount = maxScanCount;
        this.withoutVectorData = withoutVectorData;
        this.withScalarData = withScalarData;
        this.selectedKeys = selectedKeys;
        this.withTableData = withTableData;
        this.useScalarFilter = false;
        this.scalarForFilter = new HashMap<>();
    }
}
