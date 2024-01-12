package io.dingodb.sdk.service;

import io.dingodb.sdk.common.codec.CodecUtils;
import io.dingodb.sdk.common.utils.ByteArrayUtils;
import io.dingodb.sdk.common.utils.Optional;
import io.dingodb.sdk.service.entity.Message;
import io.dingodb.sdk.service.entity.common.Location;
import io.dingodb.sdk.service.entity.common.Range;
import io.dingodb.sdk.service.entity.common.Region;
import io.dingodb.sdk.service.entity.common.RegionDefinition;
import io.dingodb.sdk.service.entity.common.RegionEpoch;
import io.dingodb.sdk.service.entity.coordinator.QueryRegionRequest;
import io.dingodb.sdk.service.entity.coordinator.QueryRegionResponse;
import io.dingodb.sdk.service.entity.coordinator.ScanRegionsRequest;
import io.dingodb.sdk.service.entity.coordinator.ScanRegionsResponse;
import io.dingodb.sdk.service.entity.store.Context;
import io.grpc.Channel;
import lombok.Getter;

import static io.dingodb.sdk.common.utils.ByteArrayUtils.compare;

public class RegionChannelProvider implements ChannelProvider {

    @Getter
    private final long regionId;
    @Getter
    private Range range;
    @Getter
    private byte[] idKey;
    private byte[] nextIdKey;
    private final CoordinatorService coordinatorService;
    private Location location;
    private Channel channel;
    private RegionEpoch regionEpoch;

    public RegionChannelProvider(CoordinatorService coordinatorService, long regionId) {
        this.coordinatorService = coordinatorService;
        this.regionId = regionId;
        refresh(null, 0);
    }

    @Override
    public Channel channel() {
        return channel;
    }

    @Override
    public synchronized void refresh(Channel channel, long trace) {
        if (channel == this.channel) {
            refresh(trace);
        }
    }

    @Override
    public void before(Message.Request message) {
        if (message instanceof Message.StoreRequest) {
            Context context = ((Message.StoreRequest) message).getContext();
            if (context == null) {
                ((Message.StoreRequest) message).setContext(context = new Context());
            }
            context.setRegionEpoch(regionEpoch);
            context.setRegionId(regionId);
        }
    }

    public boolean isIn(byte[] key) {
        if (range == null) {
            throw new RuntimeException("Not refresh!");
        }
        // range.start <= key < range.end
        return compare(range.getStartKey(), key) <= 0 && compare(key, range.getEndKey()) < 0;
    }

    private void refresh(long trace) {
        if (idKey == null) {
            refreshIdKey(trace);
        }
        Optional.ofNullable(coordinatorService.scanRegions(
            trace, ScanRegionsRequest.builder().key(idKey).rangeEnd(nextIdKey).build()
        )).map(ScanRegionsResponse::getRegions)
            .map($ -> $.stream().filter(region -> region.getRegionId() == regionId).findAny().orElse(null))
            .filter($ -> $.getLeader() != null)
            .ifPresent($ -> {
                location = $.getLeader();
                channel = ChannelManager.getChannel(location);
                regionEpoch = $.getRegionEpoch();
                range = $.getRange();
            });
    }

    private synchronized void refreshIdKey(long trace) {
        if (idKey != null) {
            return;
        }
        Optional.ofNullable(coordinatorService.queryRegion(
                trace, QueryRegionRequest.builder().regionId(regionId).build())
            ).map(QueryRegionResponse::getRegion)
            .map(Region::getDefinition)
            .map(RegionDefinition::getRange)
            .ifPresent(range -> this.range = range)
            .map(Range::getStartKey)
            .ifPresent($ -> {
                long id = CodecUtils.readId($);
                this.idKey = CodecUtils.encodeId($[0], id);
                this.nextIdKey = CodecUtils.encodeId($[0], id + 1);
            });
    }

}
