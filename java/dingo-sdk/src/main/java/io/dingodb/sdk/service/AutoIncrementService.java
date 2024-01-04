package io.dingodb.sdk.service;

import io.dingodb.sdk.service.entity.common.Location;
import io.dingodb.sdk.service.entity.meta.DingoCommonId;
import io.dingodb.sdk.service.entity.meta.GenerateAutoIncrementRequest;
import io.dingodb.sdk.service.entity.meta.GenerateAutoIncrementResponse;
import io.dingodb.sdk.service.entity.meta.GetAutoIncrementRequest;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class AutoIncrementService {
    
    private static final Map<Set<Location>, Map<DingoCommonId, AutoIncrement>> cache = new ConcurrentHashMap<>();

    @AllArgsConstructor
    protected static class Increment {
        public final long limit;
        public final long inc;
    }

    @EqualsAndHashCode(onlyExplicitlyIncluded = true)
    protected class AutoIncrement {
        @EqualsAndHashCode.Include
        private final DingoCommonId tableId;
        private final int increment;
        private final int offset;

        private long limit = 0;
        private long inc = 0;

        public AutoIncrement(DingoCommonId tableId, int increment, int offset) {
            this.tableId = tableId;
            this.increment = increment;
            this.offset = offset;
        }

        public long current() {
            return inc;
        }

        public synchronized long inc() {
            long current = inc;
            if (current >= limit) {
                current = fetch();
            }
            inc += increment;
            return current;
        }

        private long fetch() {
            Increment increment = AutoIncrementService.this.fetch(tableId);
            if ((increment.inc + this.increment) >= increment.limit) {
                throw new RuntimeException("Fetch zero increment, table id: {}" + tableId);
            }
            if (increment.inc % this.offset != 0) {
                this.inc = increment.inc + this.offset - increment.inc % this.offset;
            } else {
                this.inc = increment.inc;
            }
            this.limit = increment.limit;
            return inc;
        }

    }

    private final MetaService metaService;
    private final Map<DingoCommonId, AutoIncrement> innerCache;
    private Long count = 10000L;
    private Integer increment = 1;
    private Integer offset = 1;

    public AutoIncrementService(String servers) {
        Set<Location> coordinators = Services.parse(servers);
        this.metaService = Services.autoIncrementMetaService(coordinators);
        innerCache = cache.computeIfAbsent(coordinators, s -> new ConcurrentHashMap<>());
    }

    public AutoIncrementService(Set<Location> coordinators) {
        this.metaService = Services.autoIncrementMetaService(coordinators);
        innerCache = cache.computeIfAbsent(coordinators, s -> new ConcurrentHashMap<>());
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

    private Increment fetch(DingoCommonId tableId) {
        try {
            GenerateAutoIncrementResponse response = metaService.generateAutoIncrement(GenerateAutoIncrementRequest
                .builder()
                .tableId(tableId)
                .count(count)
                .autoIncrementIncrement(increment)
                .autoIncrementOffset(offset)
                .build()
            );
            log.info(
                "Generated {} auto increment count: {}, increment: {}, offset:{}, startId:{}, endId:{}",
                tableId , count, increment, offset, response.getStartId(), response.getEndId()
            );
            return new Increment(response.getEndId(), response.getStartId());
        } catch (Exception e) {
            innerCache.remove(tableId);
            throw e;
        }
    }

    public long current(DingoCommonId tableId) {
        return metaService.getAutoIncrement(GetAutoIncrementRequest.builder().tableId(tableId).build()).getStartId();
    }

    public long localCurrent(DingoCommonId tableId) {
        AutoIncrement autoIncrement = innerCache.get(tableId);
        if (autoIncrement == null) {
            return current(tableId);
        }
        return autoIncrement.current();
    }

    public long next(DingoCommonId tableId) {
        return innerCache.computeIfAbsent(tableId, id -> new AutoIncrement(id, increment, offset)).inc();
    }

}
