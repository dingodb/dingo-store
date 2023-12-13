package io.dingodb.sdk.service;

import io.dingodb.sdk.service.entity.Message;
import io.grpc.Channel;

public interface ChannelProvider {

    Channel channel();

    void refresh(Channel channel, long trace);

    default void before(Message.Request message) {
    }

    default void after(Message.Response response) {
    }

}
