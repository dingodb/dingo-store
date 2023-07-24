package io.dingodb.sdk.common.vector;

import io.dingodb.sdk.common.index.VectorIndexParameter;
import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public class VectorIndexMetrics {

    private VectorIndexParameter.VectorIndexType vectorIndexType;
    private Long currentCount;
    private Long deletedCount;
    private Long maxId;
    private Long minId;
    private Long memoryBytes;

    public VectorIndexMetrics merge(VectorIndexMetrics other) {
        vectorIndexType = other.getVectorIndexType();
        currentCount = Long.sum(this.currentCount, other.currentCount);
        deletedCount = Long.sum(this.deletedCount, other.deletedCount);
        memoryBytes = Long.sum(this.memoryBytes, other.memoryBytes);
        maxId = Long.max(this.maxId, other.maxId);
        minId = Long.min(this.minId, other.maxId);
        return this;
    }
}
