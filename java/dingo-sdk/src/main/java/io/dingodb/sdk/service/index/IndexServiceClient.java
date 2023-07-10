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

package io.dingodb.sdk.service.index;

import io.dingodb.common.Common;
import io.dingodb.index.Index;
import io.dingodb.index.IndexServiceGrpc;
import io.dingodb.sdk.common.DingoCommonId;
import io.dingodb.sdk.common.Location;
import io.dingodb.sdk.common.SDKCommonId;
import io.dingodb.sdk.common.utils.EntityConversion;
import io.dingodb.sdk.common.vector.VectorSearchParameter;
import io.dingodb.sdk.common.vector.VectorWithDistance;
import io.dingodb.sdk.common.vector.VectorWithId;
import io.dingodb.sdk.common.table.RangeDistribution;
import io.dingodb.sdk.service.connector.IndexServiceConnector;
import io.dingodb.sdk.service.meta.MetaServiceClient;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static io.dingodb.sdk.common.utils.EntityConversion.mapping;

@Slf4j
public class IndexServiceClient {

    private final Map<DingoCommonId, IndexServiceConnector> connectorCache = new ConcurrentHashMap<>();
    private final MetaServiceClient rootMetaService;

    private Integer retryTimes;

    public IndexServiceClient(MetaServiceClient rootMetaService) {
        this(rootMetaService, 20);
    }

    public IndexServiceClient(MetaServiceClient rootMetaService, Integer retryTimes) {
        this.rootMetaService = rootMetaService;
        this.retryTimes = retryTimes;
    }

    private Supplier<Location> locationSupplier(DingoCommonId schemaId, DingoCommonId indexId, DingoCommonId regionId) {
        return () -> rootMetaService.getSubMetaService(schemaId).getIndexRangeDistribution(indexId).values().stream()
                .filter(rd -> rd.getId().equals(regionId))
                .findAny()
                .map(RangeDistribution::getLeader)
                .orElse(null);
    }

    public IndexServiceConnector getIndexStoreConnector(DingoCommonId indexId, DingoCommonId regionId) {
        SDKCommonId schemaId = new SDKCommonId(
                DingoCommonId.Type.ENTITY_TYPE_INDEX, rootMetaService.id().getEntityId(), indexId.parentId());

        return connectorCache.computeIfAbsent(
                regionId, __ -> new IndexServiceConnector(locationSupplier(schemaId, indexId, regionId)));
    }

    public void shutdown() {
        connectorCache.clear();
    }

    public boolean vectorAdd(
            DingoCommonId indexId,
            DingoCommonId regionId,
            List<VectorWithId> vectors,
            boolean replaceDeleted,
            boolean isUpdate) {
        Index.VectorAddRequest request = Index.VectorAddRequest.newBuilder()
                .setRegionId(regionId.entityId())
                .addAllVectors(vectors.stream().map(EntityConversion::mapping).collect(Collectors.toList()))
                .setReplaceDeleted(replaceDeleted)
                .setIsUpdate(isUpdate)
                .build();

        exec(stub -> stub.vectorAdd(request), retryTimes, indexId, regionId);
        return true;
    }

    public List<VectorWithDistance> vectorSearch(
            DingoCommonId indexId,
            DingoCommonId regionId,
            VectorWithId vector,
            VectorSearchParameter parameter) {
        Index.VectorSearchRequest request = Index.VectorSearchRequest.newBuilder()
                .setRegionId(regionId.entityId())
                .setVector(mapping(vector))
                .setParameter(Common.VectorSearchParameter.newBuilder()
                        .setTopN(parameter.getTopN())
                        .setWithScalarData(parameter.isWithScalarData())
                        .addAllSelectedKeys(parameter.getSelectedKeys())
                        .build())
                .build();

        Index.VectorSearchResponse response = exec(stub -> stub.vectorSearch(request), retryTimes, indexId, regionId);

        return response.getResultsList().stream().map(EntityConversion::mapping).collect(Collectors.toList());
    }

    public List<VectorWithId> vectorBatchQuery(
            DingoCommonId indexId,
            DingoCommonId regionId,
            List<Long> vectorIds,
            Boolean withScalarData,
            List<String> selectedKeys) {
        Index.VectorBatchQueryRequest request = Index.VectorBatchQueryRequest.newBuilder()
                .setRegionId(regionId.entityId())
                .addAllVectorIds(vectorIds)
                .setWithScalarData(withScalarData)
                .addAllSelectedKeys(selectedKeys)
                .build();

        Index.VectorBatchQueryResponse response = exec(stub -> stub.vectorBatchQuery(request), retryTimes, indexId,
                regionId);

        return response.getVectorsList().stream().map(EntityConversion::mapping).collect(Collectors.toList());
    }

    public Long vectorGetBoderId(DingoCommonId indexId, DingoCommonId regionId, Boolean getMin) {
        Index.VectorGetBorderIdRequest request = Index.VectorGetBorderIdRequest.newBuilder()
                .setRegionId(regionId.entityId())
                .setGetMin(getMin)
                .build();

        Index.VectorGetBorderIdResponse response = exec(stub -> stub.vectorGetBorderId(request), retryTimes, indexId,
                regionId);
        return response.getId();
    }

    public boolean vectorDelete(DingoCommonId indexId, DingoCommonId regionId, List<Long> ids) {
        Index.VectorDeleteRequest request = Index.VectorDeleteRequest.newBuilder()
                .setRegionId(regionId.entityId())
                .addAllIds(ids)
                .build();

        exec(stub -> stub.vectorDelete(request), retryTimes, indexId, regionId);

        return true;
    }

    private <R> R exec(
            Function<IndexServiceGrpc.IndexServiceBlockingStub, R> function,
            int retryTimes,
            DingoCommonId indexId,
            DingoCommonId regionId) {
        return getIndexStoreConnector(indexId, regionId).exec(function, retryTimes);
    }
}
