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
import io.dingodb.sdk.common.Context;
import io.dingodb.sdk.common.DingoCommonId;
import io.dingodb.sdk.common.Location;
import io.dingodb.sdk.common.SDKCommonId;
import io.dingodb.sdk.common.table.RangeDistribution;
import io.dingodb.sdk.common.utils.EntityConversion;
import io.dingodb.sdk.common.utils.ErrorCodeUtils;
import io.dingodb.sdk.common.utils.Optional;
import io.dingodb.sdk.common.utils.Parameters;
import io.dingodb.sdk.common.vector.Search;
import io.dingodb.sdk.common.vector.VectorIndexMetrics;
import io.dingodb.sdk.common.vector.VectorScanQuery;
import io.dingodb.sdk.common.vector.VectorSearchParameter;
import io.dingodb.sdk.common.vector.VectorWithDistanceResult;
import io.dingodb.sdk.common.vector.VectorWithId;
import io.dingodb.sdk.service.connector.IndexServiceConnector;
import io.dingodb.sdk.service.meta.MetaServiceClient;
import lombok.extern.slf4j.Slf4j;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static io.dingodb.sdk.common.utils.EntityConversion.mapping;
import static io.dingodb.sdk.common.utils.StackTraces.CURRENT_STACK;
import static io.dingodb.sdk.common.utils.StackTraces.stack;

@Slf4j
public class IndexServiceClient {

    private final Map<DingoCommonId, IndexServiceConnector> connectorCache = new ConcurrentHashMap<>();
    private final Map<DingoCommonId, Context> contextCache = new ConcurrentHashMap<>();
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
                .map(this::cacheRangeEpoch)
                .map(RangeDistribution::getLeader)
                .orElse(null);
    }

    private RangeDistribution cacheRangeEpoch(RangeDistribution rangeDistribution) {
        Context context = contextCache.get(rangeDistribution.getId());
        if (context != null && !context.getRegionEpoch().equals(rangeDistribution.getRegionEpoch())) {
            context.setRegionEpoch(rangeDistribution.getRegionEpoch());
        } else {
            contextCache.put(rangeDistribution.getId(), new Context(rangeDistribution.getId(), rangeDistribution.getRegionEpoch()));
        }
        return rangeDistribution;
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

        Index.VectorAddRequest.Builder builder = Index.VectorAddRequest.newBuilder()
                .addAllVectors(vectors.stream().map(EntityConversion::mapping).collect(Collectors.toList()))
                .setReplaceDeleted(replaceDeleted)
                .setIsUpdate(isUpdate);

        Index.VectorAddResponse response = exec(stub -> stub.vectorAdd(
                builder.setContext(mapping(contextCache.get(regionId))).build()),
                retryTimes,
                indexId,
                regionId);

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
                .setWithoutScalarData(parameter.isWithoutScalarData())
                .addAllSelectedKeys(parameter.getSelectedKeys())
                .setWithoutTableData(parameter.isWithoutTableData())
                .setVectorCoprocessor(Optional.mapOrGet(
                        parameter.getCoprocessor(),
                        __ -> mapping(parameter.getCoprocessor(), regionId.parentId()),
                        () -> Common.CoprocessorV2.newBuilder().build()))
                .addAllVectorIds(Parameters.cleanNull(parameter.getVectorIds(), Collections.emptyList()));
        if (parameter.getVectorFilter() != null) {
            builder.setVectorFilter(Common.VectorFilter.valueOf(parameter.getVectorFilter().name()));
        }
        if (parameter.getVectorFilterType() != null) {
            builder.setVectorFilterType(Common.VectorFilterType.valueOf(parameter.getVectorFilterType().name()));
        }
        if (search != null) {
            if (search.getFlat() != null) {
                builder.setFlat(Common.SearchFlatParam.newBuilder()
                        .setParallelOnQueries(search.getFlat().getParallelOnQueries())
                        .build());
            }
            if (search.getIvfFlatParam() != null) {
                builder.setIvfFlat(Common.SearchIvfFlatParam.newBuilder()
                        .setNprobe(search.getIvfFlatParam().getNprobe())
                        .setParallelOnQueries(search.getIvfFlatParam().getParallelOnQueries())
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
        }
        Index.VectorSearchRequest.Builder reqBuilder = Index.VectorSearchRequest.newBuilder()
                .addAllVectorWithIds(vectors.stream().map(EntityConversion::mapping).collect(Collectors.toList()))
                .setParameter(builder.build());

        Index.VectorSearchResponse response = exec(stub ->
                stub.vectorSearch(reqBuilder.setContext(mapping(contextCache.get(regionId))).build()),
                retryTimes, indexId, regionId);

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
            Boolean withOutScalarData,
            List<String> selectedKeys) {
        Index.VectorBatchQueryRequest.Builder builder = Index.VectorBatchQueryRequest.newBuilder()
                .addAllVectorIds(vectorIds)
                .setWithoutVectorData(withOutVectorData)
                .setWithoutScalarData(withOutScalarData)
                .addAllSelectedKeys(selectedKeys);

        Index.VectorBatchQueryResponse response = exec(stub ->
                stub.vectorBatchQuery(builder.setContext(mapping(contextCache.get(regionId))).build()),
                retryTimes, indexId, regionId);

        return response.getVectorsList().stream().map(EntityConversion::mapping).collect(Collectors.toList());
    }

    public Long vectorGetBoderId(DingoCommonId indexId, DingoCommonId regionId, Boolean getMin) {
        Index.VectorGetBorderIdRequest.Builder builder = Index.VectorGetBorderIdRequest.newBuilder().setGetMin(getMin);

        Index.VectorGetBorderIdResponse response = exec(stub ->
                stub.vectorGetBorderId(builder.setContext(mapping(contextCache.get(regionId))).build()),
                retryTimes, indexId, regionId);
        return response.getId();
    }

    public List<VectorWithId> vectorScanQuery(DingoCommonId indexId, DingoCommonId regionId, VectorScanQuery query) {
        Index.VectorScanQueryRequest.Builder builder = Index.VectorScanQueryRequest.newBuilder()
                .setVectorIdStart(query.getStartId())
                .setVectorIdEnd(query.getEndId())
                .setIsReverseScan(query.getIsReverseScan())
                .setMaxScanCount(query.getMaxScanCount())
                .setWithoutVectorData(query.getWithoutVectorData())
                .setWithoutScalarData(query.getWithoutScalarData())
                .addAllSelectedKeys(query.getSelectedKeys())
                .setWithoutTableData(query.getWithoutTableData())
                .setUseScalarFilter(Parameters.cleanNull(query.getUseScalarFilter(), false))
                .setScalarForFilter(Optional.mapOrGet(
                        query.getScalarForFilter(),
                        EntityConversion::mapping,
                        () -> Common.VectorScalardata.newBuilder().build()));

        Index.VectorScanQueryResponse response = exec(stub ->
                stub.vectorScanQuery(builder.setContext(mapping(contextCache.get(regionId))).build()),
                retryTimes, indexId, regionId);
        return response.getVectorsList().stream().map(EntityConversion::mapping).collect(Collectors.toList());
    }

    public VectorIndexMetrics vectorGetRegionMetrics(DingoCommonId indexId, DingoCommonId regionId) {
        Index.VectorGetRegionMetricsRequest.Builder builder = Index.VectorGetRegionMetricsRequest.newBuilder();

        Index.VectorGetRegionMetricsResponse response = exec(stub ->
                stub.vectorGetRegionMetrics(builder.setContext(mapping(contextCache.get(regionId))).build()),
                retryTimes, indexId, regionId);
        return mapping(response.getMetrics());
    }

    public List<Boolean> vectorDelete(DingoCommonId indexId, DingoCommonId regionId, List<Long> ids) {
        Index.VectorDeleteRequest.Builder builder = Index.VectorDeleteRequest.newBuilder().addAllIds(ids);

        Index.VectorDeleteResponse response = exec(stub ->
                stub.vectorDelete(builder.setContext(mapping(contextCache.get(regionId))).build()),
                retryTimes, indexId, regionId);

        return response.getKeyStatesList();
    }

    public Long vectorCount(DingoCommonId indexId, DingoCommonId regionId) {
        Index.VectorCountRequest.Builder builder = Index.VectorCountRequest.newBuilder();

        Index.VectorCountResponse response = exec(stub -> stub.vectorCount(
                builder.setContext(mapping(contextCache.get(regionId))).build()), retryTimes, indexId, regionId);

        return response.getCount();
    }

    private <R> R exec(
            Function<IndexServiceGrpc.IndexServiceBlockingStub, R> function,
            int retryTimes,
            DingoCommonId indexId,
            DingoCommonId regionId) {
        String stack = stack(CURRENT_STACK + 1);
        try {
            return getIndexStoreConnector(indexId, regionId).exec(
                stack, function, retryTimes, ErrorCodeUtils.errorToStrategyFunc
            );
        } catch (Exception e) {
            log.error(
                "Call [{}] exec error, index id: [{}], region id: [{}], msg: [{}].",
                stack, indexId, regionId, e.getMessage()
            );
            throw e;
        }
    }
}
