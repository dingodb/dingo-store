package io.dingodb.sdk.service;

import io.dingodb.sdk.common.utils.Optional;
import io.dingodb.sdk.common.utils.Parameters;
import io.dingodb.sdk.service.caller.RpcCaller;
import io.dingodb.sdk.service.desc.coordinator.CoordinatorServiceDescriptors;
import io.dingodb.sdk.service.entity.common.Location;
import io.dingodb.sdk.service.entity.coordinator.GetCoordinatorMapRequest;
import io.dingodb.sdk.service.entity.coordinator.GetCoordinatorMapResponse;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ManagedChannel;
import lombok.extern.slf4j.Slf4j;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

@Slf4j
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
    public synchronized void refresh(Channel oldChannel, long trace) {
        if (oldChannel != channel) {
            return;
        }
        AtomicBoolean findLeader = new AtomicBoolean(false);
        for (Location location : locations) {
            ManagedChannel channel = ChannelManager.getChannel(location);
            try {
                GetCoordinatorMapResponse response = requestCoordinatorMap(trace, channel);
                Optional.ofNullable(response)
                    .map(locationGetter)
                    .ifPresent(leader -> Parameters.nonNull(leader.getHost(), "location"))
                    .map(ChannelManager::getChannel)
                    .ifPresent(ch -> this.channel = ch)
                    .ifPresent($ -> findLeader.set(true))
                    .map(ch ->requestCoordinatorMap(trace, ChannelManager.getChannel(response.getLeaderLocation())))
                    .map(GetCoordinatorMapResponse::getCoordinatorLocations)
                    .filter($ -> !$.isEmpty())
                    .ifPresent($ -> locations = new HashSet<>($));
            } catch (Exception ignore) {
            }
            if (findLeader.get()) {
                break;
            }
        }
    }

    private GetCoordinatorMapResponse requestCoordinatorMap(long trace, Channel channel) {
        return RpcCaller
            .call(
                CoordinatorServiceDescriptors.getCoordinatorMap,
                new GetCoordinatorMapRequest(),
                CallOptions.DEFAULT.withDeadlineAfter(30, TimeUnit.SECONDS),
                channel,
                trace,
                CoordinatorServiceDescriptors.getCoordinatorMapHandlers
            );
    }

}
