package io.dingodb.sdk.service.connector;

import io.dingodb.sdk.common.Location;
import io.dingodb.sdk.common.utils.Optional;
import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import io.grpc.ManagedChannel;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static io.dingodb.sdk.common.utils.NoBreakFunctions.wrap;

@Slf4j
public final class ChannelManager {
    private ChannelManager() {
        Runtime.getRuntime().addShutdownHook(new Thread(ChannelManager::shutdown));
    }

    private static final Map<Location, ManagedChannel> channels = new ConcurrentHashMap<>();

    public static ManagedChannel getChannel(String host, int port) {
        return getChannel(new Location(host, port));
    }

    public static ManagedChannel getChannel(Location location) {
        return Optional.ofNullable(location)
            .filter(__ -> !__.getHost().isEmpty())
            .ifAbsent(() -> log.warn("Cannot connect empty host."))
            .map(__ -> channels.computeIfAbsent(location, k -> newChannel(k.getHost(), k.getPort())))
            .orNull();
    }

    private static ManagedChannel newChannel(String host, int port) {
        return Grpc.newChannelBuilder(host + ":" + port, InsecureChannelCredentials.create()).build();
    }

    private static void shutdown() {
        channels.values().forEach(wrap(ch -> {
            ch.shutdown();
        }, e -> log.error("Close channel error.", e)));
    }

}
