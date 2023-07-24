package io.dingodb.sdk.common.vector;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.util.List;

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
}
