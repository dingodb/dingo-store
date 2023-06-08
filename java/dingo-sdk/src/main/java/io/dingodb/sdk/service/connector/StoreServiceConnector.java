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

package io.dingodb.sdk.service.connector;

import io.dingodb.sdk.common.Location;
import io.dingodb.store.StoreServiceGrpc;
import io.grpc.ManagedChannel;

import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

public class StoreServiceConnector extends ServiceConnector<StoreServiceGrpc.StoreServiceBlockingStub> {

    private final Supplier<Location> leaderSupplier;

    public StoreServiceConnector(Supplier<Location> leaderSupplier) {
        super(Collections.emptySet());
        this.leaderSupplier = leaderSupplier;
    }

    @Deprecated
    public StoreServiceConnector(Supplier<Location> leaderSupplier, List<Location> locations) {
        this(leaderSupplier);
    }

    @Override
    protected ManagedChannel transformToLeaderChannel(ManagedChannel channel) {
        Location leader = leaderSupplier.get();
        if (leader == null || leader.getHost().isEmpty()) {
            return null;
        }
        return newChannel(leader);
    }

    @Override
    public StoreServiceGrpc.StoreServiceBlockingStub newStub(ManagedChannel channel) {
        return StoreServiceGrpc.newBlockingStub(channel);
    }

}
