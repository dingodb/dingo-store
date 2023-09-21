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

package io.dingodb.sdk.service.util;

import io.dingodb.common.Common;
import io.dingodb.index.Index;
import io.dingodb.sdk.common.DingoCommonId;
import io.dingodb.sdk.common.Location;
import io.dingodb.sdk.common.SDKCommonId;
import io.dingodb.sdk.common.table.RangeDistribution;
import io.dingodb.sdk.common.utils.EntityConversion;
import io.dingodb.sdk.common.vector.VectorCalcDistance;
import io.dingodb.sdk.common.vector.VectorDistance;
import io.dingodb.sdk.common.vector.VectorDistanceRes;
import io.dingodb.sdk.service.connector.UtilServiceConnector;
import io.dingodb.sdk.service.meta.MetaServiceClient;
import io.dingodb.util.UtilServiceGrpc;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class UtilServiceClient {

    private final Map<DingoCommonId, UtilServiceConnector> connectorCache = new ConcurrentHashMap<>();
    private final MetaServiceClient rootMetaService;
    private Integer retryTimes;

    public UtilServiceClient(MetaServiceClient rootMetaService) {
        this(rootMetaService, 20);
    }

    public UtilServiceClient(MetaServiceClient rootMetaService, Integer retryTimes) {
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

    public UtilServiceConnector getIndexStoreConnector(DingoCommonId indexId, DingoCommonId regionId) {
        SDKCommonId schemaId = new SDKCommonId(
                DingoCommonId.Type.ENTITY_TYPE_INDEX, rootMetaService.id().getEntityId(), indexId.parentId());

        return connectorCache.computeIfAbsent(
                regionId, __ -> new UtilServiceConnector(locationSupplier(schemaId, indexId, regionId)));
    }

    public void shutdown() {
        connectorCache.clear();
    }

    public VectorDistanceRes vectorCalcDistance(
            DingoCommonId indexId,
            DingoCommonId regionId,
            VectorCalcDistance distance
    ) {
        Index.VectorCalcDistanceRequest request = Index.VectorCalcDistanceRequest.newBuilder()
                .setAlgorithmType(Index.AlgorithmType.valueOf(distance.getAlgorithmType().name()))
                .setMetricType(Common.MetricType.valueOf(distance.getMetricType().name()))
                .addAllOpLeftVectors(distance.getLeftVectors()
                        .stream()
                        .map(EntityConversion::mapping)
                        .collect(Collectors.toList()))
                .addAllOpRightVectors(distance.getRightVectors()
                        .stream()
                        .map(EntityConversion::mapping)
                        .collect(Collectors.toList()))
                .setIsReturnNormlize(distance.getIsReturnNormalize())
                .build();

        Index.VectorCalcDistanceResponse response =
                exec(stub -> stub.vectorCalcDistance(request), retryTimes, indexId, regionId);

        return new VectorDistanceRes(
                response.getOpLeftVectorsList().stream().map(EntityConversion::mapping).collect(Collectors.toList()),
                response.getOpRightVectorsList().stream().map(EntityConversion::mapping).collect(Collectors.toList()),
                response.getDistancesList().stream()
                        .map(d -> new VectorDistance(d.getInternalDistancesList()))
                        .collect(Collectors.toList())
        );
    }

    private <R> R exec(
            Function<UtilServiceGrpc.UtilServiceBlockingStub, R> function,
            int retryTimes,
            DingoCommonId indexId,
            DingoCommonId regionId) {
        return getIndexStoreConnector(indexId, regionId).exec(function, retryTimes);
    }
}
