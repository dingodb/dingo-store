/*
 * Copyright 2021 DataCanvas
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.dingodb.sdk.service;

import io.dingodb.sdk.common.utils.Optional;
import io.dingodb.sdk.common.utils.StackTraces;
import io.dingodb.sdk.service.entity.common.Location;
import io.grpc.InsecureChannelCredentials;
import io.grpc.ManagedChannel;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static io.dingodb.sdk.common.utils.NoBreakFunctions.wrap;

@Slf4j
public final class ChannelManager {

    public static final int DEFAULT_MAX_MESSAGE_SIZE = 1024 * 1024 * 1024;

    private ChannelManager() {
        Runtime.getRuntime().addShutdownHook(new Thread(ChannelManager::shutdown));
    }

    private static final Map<Location, ManagedChannel> channels = new ConcurrentHashMap<>();

    public static ManagedChannel getChannel(String host, int port) {
        return getChannel(Location.builder().host(host).port(port).build());
    }

    public static ManagedChannel getChannel(io.dingodb.sdk.common.Location location) {
        return Optional.ofNullable(location)
            .filter(__ -> __.getHost() != null)
            .filter(__ -> !__.getHost().isEmpty())
            .ifAbsent(() -> {
                if (log.isDebugEnabled()) {
                    log.warn("Cannot connect empty host, call stack {}.", StackTraces.stack(1));
                }
            })
            .map(__ -> channels.computeIfAbsent(
                Location.builder().host(location.getHost()).port(location.getPort()).build(),
                k -> newChannel(k.getHost(), k.getPort())
            )).orNull();
    }

    public static ManagedChannel getChannel(Location location) {
        return Optional.ofNullable(location)
            .filter(__ -> __.getHost() != null)
            .filter(__ -> !__.getHost().isEmpty())
            .ifAbsent(() -> {
                if (log.isDebugEnabled()) {
                    log.warn("Cannot connect empty host, call stack {}.", StackTraces.stack(1));
                }
            })            .map(__ -> channels.computeIfAbsent(location, k -> newChannel(k.getHost(), k.getPort())))
            .orNull();
    }

    private static ManagedChannel newChannel(String host, int port) {
        return NettyChannelBuilder.forAddress(host, port, InsecureChannelCredentials.create())
            .flowControlWindow(DEFAULT_MAX_MESSAGE_SIZE)
            .maxInboundMessageSize(DEFAULT_MAX_MESSAGE_SIZE)
            .maxInboundMetadataSize(DEFAULT_MAX_MESSAGE_SIZE)
            .keepAliveWithoutCalls(true)
            .build();
    }

    private static void shutdown() {
        channels.values().forEach(wrap(ch -> {
            ch.shutdown();
        }, e -> log.error("Close channel error.", e)));
    }

}
