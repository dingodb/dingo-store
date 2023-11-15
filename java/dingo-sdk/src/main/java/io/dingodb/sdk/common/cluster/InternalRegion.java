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

public class InternalRegion implements Region {
    public int regionState;

    public int regionType;
    public long createTime;
    public long deleteTime;

    public InternalRegion(int regionState, int regionType, long createTime, long deleteTime) {
        this.regionState = regionState;
        this.regionType = regionType;
        this.createTime = createTime;
        this.deleteTime = deleteTime;
    }

    @Override
    public int regionState() {
        return regionState;
    }

    @Override
    public int regionType() {
        return regionType;
    }

    @Override
    public long createTime() {
        return createTime;
    }

    @Override
    public long deleteTime() {
        return deleteTime;
    }
}
