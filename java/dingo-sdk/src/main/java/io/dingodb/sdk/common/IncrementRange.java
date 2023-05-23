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

package io.dingodb.sdk.common;

import lombok.Getter;
import lombok.ToString;

import java.util.concurrent.atomic.AtomicLong;

@Getter
@ToString
public class IncrementRange {

    private final DingoCommonId tableId;
    private final Long count;
    private final Integer increment;
    private final Integer offset;

    private AtomicLong startId;
    private AtomicLong endId;

    public IncrementRange(DingoCommonId tableId, Long count, Integer increment, Integer offset) {
        this.tableId = tableId;
        this.count = count;
        this.increment = increment;
        this.offset = offset;
    }

    public IncrementRange setStartId(long startId) {
        this.startId = new AtomicLong(startId == offset ? startId : (startId + offset) - 1);
        return this;
    }

    public IncrementRange setEndId(long endId) {
        this.endId = new AtomicLong(endId);
        return this;
    }

    public Long getAndIncrement() {
        long incrementId = startId.getAndAdd(increment);
        return incrementId >= endId.get() ? null : incrementId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        IncrementRange that = (IncrementRange) o;

        return tableId != null ? tableId.equals(that.tableId) : that.tableId == null;
    }

    @Override
    public int hashCode() {
        return tableId != null ? tableId.hashCode() : 0;
    }
}
