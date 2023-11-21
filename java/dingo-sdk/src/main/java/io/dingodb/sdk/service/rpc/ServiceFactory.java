package io.dingodb.sdk.service.rpc;

import io.dingodb.sdk.common.utils.Parameters;
import io.dingodb.sdk.service.rpc.message.common.Location;
import io.dingodb.sdk.service.rpc.message.coordinator.GetCoordinatorMapRequest;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.stub.ClientCalls;

import java.util.Arrays;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

public class ServiceFactory {

    public static final int DEFAULT_RETRY_TIMES = 30;

    private static Function<Channel, Channel> coordinatorLeaderProvider = ch -> {
        Location location = ClientCalls.blockingUnaryCall(ch,
            CoordinatorService.getCoordinatorMap, CallOptions.DEFAULT,
            new GetCoordinatorMapRequest()
        ).leaderLocation();
        if (!location.host().isEmpty()) {
            return ChannelManager.getChannel(location);
        }
        return null;
    };

    private static Function<Channel, Channel> coordinatorKvLeaderProvider = ch -> {
        Location location = ClientCalls.blockingUnaryCall(ch,
            CoordinatorService.getCoordinatorMap, CallOptions.DEFAULT.withDeadlineAfter(1, TimeUnit.SECONDS),
            new GetCoordinatorMapRequest()
        ).kvLeaderLocation();
        if (!location.host().isEmpty()) {
            return ChannelManager.getChannel(location);
        }
        return null;
    };

    public static Set<Location> parse(String locations) {
        return Arrays.stream(locations.split(","))
            .map(ss -> ss.split(":"))
            .map(ss -> new Location().host(ss[0]).port(Integer.parseInt(ss[1])))
            .collect(Collectors.toSet());
    }

    public static CoordinatorService coordinatorService(Set<Location> locations) {
        return new RpcConnector<CoordinatorService>(
            locations, DEFAULT_RETRY_TIMES, coordinatorLeaderProvider
        ){}.createCaller();
    }

    public static CoordinatorService coordinatorService(Set<Location> locations, int retryTimes) {
        return new RpcConnector<CoordinatorService>(
            Parameters.notEmpty(locations, "locations"), retryTimes, coordinatorLeaderProvider
        ){}.createCaller();
    }

    public static VersionService versionService(Set<Location> locations, int retryTimes) {
        return new RpcConnector<VersionService>(
            Parameters.notEmpty(locations, "locations"), retryTimes, coordinatorKvLeaderProvider
        ){}.createCaller();
    }

}
