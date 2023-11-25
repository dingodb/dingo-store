package io.dingodb.sdk.service;

import io.grpc.Channel;

public interface ChannelProvider {

    Channel channel();

    void refresh(Channel channel, long trace);

}
