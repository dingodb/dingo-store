package io.dingodb.sdk.service;

import io.dingodb.sdk.service.entity.Message;
import io.dingodb.sdk.service.entity.common.Location;
import io.dingodb.sdk.service.entity.common.RegionEpoch;
import io.dingodb.sdk.service.entity.coordinator.QueryRegionRequest;
import io.dingodb.sdk.service.entity.coordinator.ScanRegionInfo;
import io.dingodb.sdk.service.entity.coordinator.ScanRegionsRequest;
import io.dingodb.sdk.service.entity.meta.DingoCommonId;
import io.dingodb.sdk.service.entity.store.Context;
import io.grpc.Channel;

public class RegionChannelProvider implements ChannelProvider {

    private final CoordinatorService coordinatorService;
    private final long regionId;
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
        if (channel == null || channel != this.channel) {
            refresh();
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

    private void refresh() {
        byte[] startKey = coordinatorService.queryRegion(
            QueryRegionRequest.builder().regionId(regionId).build()
        ).getRegion().getDefinition().getRange().getStartKey();
        ScanRegionInfo regionInfo = coordinatorService.scanRegions(
            ScanRegionsRequest.builder().key(startKey).build()
        ).getRegions().get(0);
        if (regionInfo != null && regionInfo.getLeader() != null) {
            location = regionInfo.getLeader();
            channel = ChannelManager.getChannel(location);
            regionEpoch = regionInfo.getRegionEpoch();
        }
    }

}
