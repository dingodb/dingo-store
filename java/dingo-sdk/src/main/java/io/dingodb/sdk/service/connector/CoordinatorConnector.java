/*
 * Copyright 2021, Zetyun DataPortal All rights reserved.
 */

package io.dingodb.sdk.service.connector;

import io.dingodb.common.Common;
import io.dingodb.sdk.common.utils.Optional;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;

public class CoordinatorConnector extends ServiceConnector {

    public CoordinatorConnector(List<Common.Location> locations) {
        super(new HashSet<>(locations));
    }

    public static CoordinatorConnector getCoordinatorConnector(String target) {
        return Optional.ofNullable(target.split(","))
            .map(Arrays::stream)
            .map(ss -> ss
                .map(s -> s.split(":"))
                .map(__ -> Common.Location.newBuilder().setHost(__[0]).setPort(Integer.parseInt(__[1])).build())
                .collect(Collectors.toList()))
            .map(CoordinatorConnector::new).orNull();
    }
}
