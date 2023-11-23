package io.dingodb.sdk.service;

import io.dingodb.sdk.common.utils.Optional;
import io.dingodb.sdk.common.utils.Parameters;
import io.dingodb.sdk.service.caller.RpcCaller;
import io.dingodb.sdk.service.entity.common.Location;
import io.dingodb.sdk.service.entity.coordinator.GetCoordinatorMapRequest;
import io.dingodb.sdk.service.entity.coordinator.GetCoordinatorMapResponse;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ManagedChannel;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

public class CoordinatorChannelProvider implements ChannelProvider {

    private final Function<GetCoordinatorMapResponse, Location> locationGetter;

    private Set<Location> locations;
    private Channel channel;

    public CoordinatorChannelProvider(
        Set<Location> locations, Function<GetCoordinatorMapResponse, Location> locationGetter
    ) {
        this.locations = locations;
        this.locationGetter = locationGetter;
    }

    @Override
    public synchronized Channel channel() {
        return channel;
    }

    @Override
    public synchronized void refresh(Channel oldChannel) {
        if (oldChannel != channel) {
            return;
        }
        Optional<ManagedChannel> channelOptional = Optional.empty();
        for (Location location : locations) {
            ManagedChannel channel = ChannelManager.getChannel(location);
            try {
                GetCoordinatorMapResponse response = RpcCaller
                    .call(
                        CoordinatorService.getCoordinatorMap,
                        new GetCoordinatorMapRequest(),
                        CallOptions.DEFAULT.withDeadlineAfter(3, TimeUnit.SECONDS),
                        channel
                    );
                channelOptional = Optional.of(response)
                    .ifPresent(res -> locations = new HashSet<>(res.getCoordinatorLocations()))
                    .map(locationGetter)
                    .ifPresent(leader -> Parameters.nonNull(leader.getHost(), "location"))
                    .map(ChannelManager::getChannel)
                    .ifPresent(ch -> this.channel = ch);
            } catch (Exception ignore) {
            }
            if (channelOptional.isPresent()) {
                break;
            }
        }
    }

}
