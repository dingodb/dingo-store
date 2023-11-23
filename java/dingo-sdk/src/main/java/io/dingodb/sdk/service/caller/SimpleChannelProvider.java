package io.dingodb.sdk.service.caller;

import io.grpc.Channel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@AllArgsConstructor
public class SimpleChannelProvider {
    private Channel channel;
}
