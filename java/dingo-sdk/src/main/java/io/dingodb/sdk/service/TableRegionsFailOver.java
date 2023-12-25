package io.dingodb.sdk.service;

import io.dingodb.sdk.service.entity.Message;
import io.dingodb.sdk.service.entity.common.Location;
import io.dingodb.sdk.service.entity.common.RegionEpoch;
import io.dingodb.sdk.service.entity.meta.DingoCommonId;
import io.dingodb.sdk.service.entity.meta.GetIndexRangeRequest;
import io.dingodb.sdk.service.entity.meta.GetTableRangeRequest;
import io.dingodb.sdk.service.entity.meta.RangeDistribution;
import io.dingodb.sdk.service.entity.store.Context;
import io.grpc.Channel;
import lombok.extern.slf4j.Slf4j;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.Collectors;

import static io.dingodb.sdk.service.entity.meta.EntityType.ENTITY_TYPE_TABLE;


@Slf4j
public class TableRegionsFailOver {

    private final DingoCommonId tableId;
    private final MetaService metaService;

    private final AtomicBoolean refresh = new AtomicBoolean();

    private Map<DingoCommonId, RangeDistribution> regionChannels = Collections.emptyMap();

    private final Function<Long, List<RangeDistribution>> rangesGetter;

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
        private RegionEpoch regionEpoch;

        private RegionChannelProvider(DingoCommonId regionId) {
            this.regionId = regionId;
            refresh(null, 0);
        }

        @Override
        public Channel channel() {
            return channel;
        }

        @Override
        public synchronized void refresh(Channel channel, long trace) {
            if (channel != null || location != null || regionEpoch != null) {
                TableRegionsFailOver.this.refresh(regionId, location, trace);
            }
            RangeDistribution distribution = regionChannels.get(regionId);
            if (distribution == null || distribution.getLeader() == null) {
                TableRegionsFailOver.this.refresh(regionId, location, trace);
                if ((distribution = regionChannels.get(regionId)) == null) {
                    return;
                }
            }
            this.location = distribution.getLeader();
            this.channel = ChannelManager.getChannel(location);
            this.regionEpoch = distribution.getRegionEpoch();
        }

        @Override
        public void before(Message.Request message) {
            if (message instanceof Message.StoreRequest) {
                Context context = ((Message.StoreRequest) message).getContext();
                if (context == null) {
                    ((Message.StoreRequest) message).setContext(context = new Context());
                }
                context.setRegionEpoch(regionEpoch);
                context.setRegionId(regionId.getEntityId());
            }
        }
    }

    private List<RangeDistribution> tableGetter(long trace) {
        return metaService.getTableRange(trace, GetTableRangeRequest.builder().tableId(tableId).build())
            .getTableRange().getRangeDistribution();
    }

    private List<RangeDistribution> indexGetter(long trace) {
        return metaService.getIndexRange(trace, GetIndexRangeRequest.builder().indexId(tableId).build())
            .getIndexRange().getRangeDistribution();
    }

    public void refresh(DingoCommonId regionId, Location location, long trace) {
        if (!refresh.compareAndSet(false, true)) {
            return;
        }
        try {
            if (!regionChannels.containsKey(regionId) || regionChannels.get(regionId).getLeader() == null || regionChannels.get(regionId).getLeader().equals(location)) {
                regionChannels = rangesGetter.apply(trace).stream().collect(Collectors.toMap(
                    RangeDistribution::getId, $ -> $
                ));
            }
        } catch (Exception e) {
            if (log.isDebugEnabled()) {
                log.warn("Get table ranges failed, table id: [{}], will retry...", tableId, e);
            }
        } finally {
            refresh.set(false);
        }
    }

}
