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

package io.dingodb.sdk.common.cluster;

import io.dingodb.sdk.common.Location;

public class InternalStore implements Store {
    private long id;
    private int storeType;
    private int storeState;
    private Location serverLocation;
    private Location raftLocation;

    public InternalStore(long id, int storeType, int storeState, Location serverLocation, Location raftLocation) {
        this.id = id;
        this.storeType = storeType;
        this.storeState = storeState;
        this.serverLocation = serverLocation;
        this.raftLocation = raftLocation;
    }

    @Override
    public long id() {
        return id;
    }

    @Override
    public int storeType() {
        return storeType;
    }

    @Override
    public Location serverLocation() {
        return serverLocation;
    }

    @Override
    public Location raftLocation() {
        return raftLocation;
    }

    @Override
    public int storeState() {
        return storeState;
    }
}
