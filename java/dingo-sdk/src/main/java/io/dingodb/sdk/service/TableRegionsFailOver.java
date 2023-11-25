package io.dingodb.sdk.service;

import io.dingodb.sdk.service.entity.common.Location;
import io.dingodb.sdk.service.entity.meta.DingoCommonId;
import io.dingodb.sdk.service.entity.meta.GetIndexRangeRequest;
import io.dingodb.sdk.service.entity.meta.GetTableRangeRequest;
import io.dingodb.sdk.service.entity.meta.RangeDistribution;
import io.grpc.Channel;
import lombok.extern.slf4j.Slf4j;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static io.dingodb.sdk.service.entity.meta.EntityType.ENTITY_TYPE_TABLE;


@Slf4j
public class TableRegionsFailOver {

    private final DingoCommonId tableId;
    private final MetaService metaService;

    private final AtomicBoolean refresh = new AtomicBoolean();

    private Map<DingoCommonId, Location> regionChannels = Collections.emptyMap();

    private final Supplier<List<RangeDistribution>> rangesGetter;

    public TableRegionsFailOver(DingoCommonId tableId, MetaService metaService) {
        this.tableId =tableId;
        this.metaService = metaService;
        this.rangesGetter = tableId.getEntityType() == ENTITY_TYPE_TABLE ? this::tableGetter : this::indexGetter;
    }

    public ChannelProvider createRegionProvider(DingoCommonId regionId) {
        return new RegionChannelProvider(regionId);
    }

    public class RegionChannelProvider implements ChannelProvider {

        private final DingoCommonId regionId;
        private Location location;
        private Channel channel;

        private RegionChannelProvider(DingoCommonId regionId) {
            this.regionId = regionId;
        }

        @Override
        public Channel channel() {
            return channel;
        }

        @Override
        public synchronized void refresh(Channel channel, long trace) {
            TableRegionsFailOver.this.refresh(regionId, location, trace);
            location = regionChannels.get(regionId);
            this.channel = ChannelManager.getChannel(location);
        }
    }

    private List<RangeDistribution> tableGetter() {
        return metaService.getTableRange(GetTableRangeRequest.builder().tableId(tableId).build())
            .getTableRange().getRangeDistribution();
    }

    private List<RangeDistribution> indexGetter() {
        return metaService.getIndexRange(GetIndexRangeRequest.builder().indexId(tableId).build())
            .getIndexRange().getRangeDistribution();
    }

    public void refresh(DingoCommonId regionId, Location location, long trace) {
        if (!refresh.compareAndSet(false, true)) {
            return;
        }
        try {
            if (!regionChannels.containsKey(regionId) || regionChannels.get(regionId).equals(location)) {
                regionChannels = rangesGetter.get().stream().collect(Collectors.toMap(
                    RangeDistribution::getId, RangeDistribution::getLeader
                ));
            }
        } catch (Exception e) {
            if (log.isDebugEnabled()) {
                log.warn("Get table ranges failed, table id: [{}], will retry...", tableId);
            }
        } finally {
            refresh.set(false);
        }
    }

}
