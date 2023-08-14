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
import io.dingodb.sdk.common.vector.Search;
import io.dingodb.sdk.common.vector.VectorCalcDistance;
import io.dingodb.sdk.common.vector.VectorDistance;
import io.dingodb.sdk.common.vector.VectorDistanceRes;
import io.dingodb.sdk.common.vector.VectorIndexMetrics;
import io.dingodb.sdk.common.vector.VectorScanQuery;
import io.dingodb.sdk.common.vector.VectorSearchParameter;
import io.dingodb.sdk.common.vector.VectorWithDistanceResult;
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

    public List<Boolean> vectorAdd(
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

        Index.VectorAddResponse response = exec(stub -> stub.vectorAdd(request), retryTimes, indexId, regionId);

        return response.getKeyStatesList();
    }

    public List<VectorWithDistanceResult> vectorSearch(
            DingoCommonId indexId,
            DingoCommonId regionId,
            List<VectorWithId> vectors,
            VectorSearchParameter parameter) {
        Search search = parameter.getSearch();
        Common.VectorSearchParameter.Builder builder = Common.VectorSearchParameter.newBuilder()
                .setTopN(parameter.getTopN())
                .setWithoutVectorData(parameter.isWithoutVectorData())
                .setWithScalarData(parameter.isWithScalarData())
                .addAllSelectedKeys(parameter.getSelectedKeys())
                .setUseScalarFilter(false)
                .setVectorFilter(Common.VectorFilter.valueOf(parameter.getVectorFilter().name()))
                .setVectorFilterType(Common.VectorFilterType.valueOf(parameter.getVectorFilterType().name()))
                .setVectorCoprocessor(mapping(parameter.getCoprocessor(), regionId.parentId()))
                .addAllVectorIds(parameter.getVectorIds());
        if (search.getFlat() != null) {
            builder.setFlat(Common.SearchFlatParam.newBuilder()
                    .setParallelOnQueries(search.getFlat().getParallelOnQueries())
                    .build());
        }
        if (search.getIvfFlatParam() != null) {
            builder.setIvfFlat(Common.SearchIvfFlatParam.newBuilder()
                    .setNprobe(search.getIvfFlatParam().getNprobe())
                    .setParallelOnQueries(search.getIvfPqParam().getParallelOnQueries())
                    .build());
        }
        if (search.getIvfPqParam() != null) {
            builder.setIvfPq(Common.SearchIvfPqParam.newBuilder()
                    .setNprobe(search.getIvfPqParam().getNprobe())
                    .setParallelOnQueries(search.getIvfPqParam().getParallelOnQueries())
                    .setRecallNum(search.getIvfPqParam().getRecallNum())
                    .build());
        }
        if (search.getHnswParam() != null) {
            builder.setHnsw(Common.SearchHNSWParam.newBuilder()
                    .setEfSearch(search.getHnswParam().getEfSearch())
                    .build());
        }
        if (search.getDiskAnnParam() != null) {

        }
        Index.VectorSearchRequest request = Index.VectorSearchRequest.newBuilder()
                .setRegionId(regionId.entityId())
                .addAllVectorWithIds(vectors.stream().map(EntityConversion::mapping).collect(Collectors.toList()))
                .setParameter(builder.build())
                .build();

        Index.VectorSearchResponse response = exec(stub -> stub.vectorSearch(request), retryTimes, indexId, regionId);

        return response.getBatchResultsList().stream()
                .map(r -> new VectorWithDistanceResult(r.getVectorWithDistancesList().stream()
                        .map(EntityConversion::mapping)
                        .collect(Collectors.toList())))
                .collect(Collectors.toList());
    }

    public List<VectorWithId> vectorBatchQuery(
            DingoCommonId indexId,
            DingoCommonId regionId,
            List<Long> vectorIds,
            Boolean withOutVectorData,
            Boolean withScalarData,
            List<String> selectedKeys) {
        Index.VectorBatchQueryRequest request = Index.VectorBatchQueryRequest.newBuilder()
                .setRegionId(regionId.entityId())
                .addAllVectorIds(vectorIds)
                .setWithoutVectorData(withOutVectorData)
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

    public List<VectorWithId> vectorScanQuery(DingoCommonId indexId, DingoCommonId regionId, VectorScanQuery query) {
        Index.VectorScanQueryRequest request = Index.VectorScanQueryRequest.newBuilder()
                .setRegionId(regionId.entityId())
                .setVectorIdStart(query.getStartId())
                .setIsReverseScan(query.getIsReverseScan())
                .setMaxScanCount(query.getMaxScanCount())
                .setWithoutVectorData(query.getWithoutVectorData())
                .setWithScalarData(query.getWithScalarData())
                .addAllSelectedKeys(query.getSelectedKeys())
                .setWithTableData(query.getWithTableData())
                .setUseScalarFilter(query.getUseScalarFilter())
                .setScalarForFilter(mapping(query.getScalarForFilter()))
                .build();

        Index.VectorScanQueryResponse response = exec(stub -> stub.vectorScanQuery(request), retryTimes, indexId,
                regionId);
        return response.getVectorsList().stream().map(EntityConversion::mapping).collect(Collectors.toList());
    }

    public VectorDistanceRes vectorCalcDistance(DingoCommonId indexId, DingoCommonId regionId, VectorCalcDistance distance) {
        Index.VectorCalcDistanceRequest request = Index.VectorCalcDistanceRequest.newBuilder()
                .setAlgorithmType(Index.AlgorithmType.valueOf(distance.getAlgorithmType().name()))
                .setMetricType(Common.MetricType.valueOf(distance.getMetricType().name()))
                .addAllOpLeftVectors(distance.getLeftVectors().stream().map(EntityConversion::mapping).collect(Collectors.toList()))
                .addAllOpRightVectors(distance.getRightVectors().stream().map(EntityConversion::mapping).collect(Collectors.toList()))
                .setIsReturnNormlize(distance.getIsReturnNormalize())
                .build();

        Index.VectorCalcDistanceResponse response = exec(stub -> stub.vectorCalcDistance(request), retryTimes, indexId, regionId);

        return new VectorDistanceRes(
                response.getOpLeftVectorsList().stream().map(EntityConversion::mapping).collect(Collectors.toList()),
                response.getOpRightVectorsList().stream().map(EntityConversion::mapping).collect(Collectors.toList()),
                response.getDistancesList().stream().map(d -> new VectorDistance(d.getInternalDistancesList())).collect(Collectors.toList())
        );
    }

    public VectorIndexMetrics vectorGetRegionMetrics(DingoCommonId indexId, DingoCommonId regionId) {
        Index.VectorGetRegionMetricsRequest request = Index.VectorGetRegionMetricsRequest.newBuilder()
                .setRegionId(regionId.entityId())
                .build();

        Index.VectorGetRegionMetricsResponse response = exec(stub -> stub.vectorGetRegionMetrics(request), retryTimes,
                indexId, regionId);
        return mapping(response.getMetrics());
    }

    public List<Boolean> vectorDelete(DingoCommonId indexId, DingoCommonId regionId, List<Long> ids) {
        Index.VectorDeleteRequest request = Index.VectorDeleteRequest.newBuilder()
                .setRegionId(regionId.entityId())
                .addAllIds(ids)
                .build();

        Index.VectorDeleteResponse response = exec(stub -> stub.vectorDelete(request), retryTimes, indexId, regionId);

        return response.getKeyStatesList();
    }

    private <R> R exec(
            Function<IndexServiceGrpc.IndexServiceBlockingStub, R> function,
            int retryTimes,
            DingoCommonId indexId,
            DingoCommonId regionId) {
        return getIndexStoreConnector(indexId, regionId).exec(function, retryTimes);
    }
}
