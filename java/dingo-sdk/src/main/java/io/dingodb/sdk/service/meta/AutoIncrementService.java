package io.dingodb.sdk.service.meta;

import io.dingodb.meta.Meta;
import io.dingodb.sdk.common.AutoIncrement;
import io.dingodb.sdk.common.DingoCommonId;
import io.dingodb.sdk.service.connector.AutoIncrementServiceConnector;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static io.dingodb.sdk.common.utils.EntityConversion.mapping;

@Slf4j
public class AutoIncrementService {
    
    private static final Map<String, Map<DingoCommonId, AutoIncrement>> cache = new ConcurrentHashMap<>();

    private final Map<DingoCommonId, AutoIncrement> innerCache;
    private final AutoIncrementServiceConnector connector;
    private Long count = 10000L;
    private Integer increment = 1;
    private Integer offset = 1;

    public AutoIncrementService(String servers) {
        connector = AutoIncrementServiceConnector.getAutoIncrementServiceConnector(servers);
        innerCache = cache.computeIfAbsent(servers, s -> new ConcurrentHashMap<>());
    }

    public void resetCount(long count) {
        this.count = count;
    }

    public void resetOffset(int offset) {
        this.offset = offset;
        cache.forEach((k, v) -> v.clear());
    }

    public void resetIncrement(int increment) {
        this.increment = increment;
        cache.forEach((k, v) -> v.clear());
    }

    public void reset(long count, int increment, int offset) {
        this.count = count;
        this.offset = offset;
        this.increment = increment;
        cache.forEach((k, v) -> v.clear());
    }

    private AutoIncrement.Increment fetcher(DingoCommonId tableId) {
        try {
            log.info("Generate auto-increment request count:{}, increment:{}, offset:{}", count, increment, offset);
            Meta.GenerateAutoIncrementRequest request = Meta.GenerateAutoIncrementRequest.newBuilder()
                .setTableId(mapping(tableId))
                .setCount(count)
                .setAutoIncrementIncrement(increment)
                .setAutoIncrementOffset(offset)
                .build();
            Meta.GenerateAutoIncrementResponse response = connector.exec(stub -> stub.generateAutoIncrement(request));

            log.info("Generated auto-increment response startId:{}, endId:{}",
                    response.getStartId(),
                    response.getEndId());
            return new AutoIncrement.Increment(response.getEndId(), response.getStartId());
        } catch (Exception e) {
            innerCache.remove(tableId);
            throw e;
        }
    }

    public long current(DingoCommonId tableId) {
        Meta.GetAutoIncrementRequest request = Meta.GetAutoIncrementRequest.newBuilder()
            .setTableId(mapping(tableId))
            .build();

        return connector.exec(stub -> stub.getAutoIncrement(request)).getStartId();
    }

    public long localCurrent(DingoCommonId tableId) {
        AutoIncrement autoIncrement = innerCache.get(tableId);
        if (autoIncrement == null) {
            return current(tableId);
        }
        return autoIncrement.current();
    }

    public long next(DingoCommonId tableId) {
        return innerCache.computeIfAbsent(tableId, id -> new AutoIncrement(id, increment, offset, this::fetcher)).inc();
    }

}
