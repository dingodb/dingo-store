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

package io.dingodb.client.operation;

import io.dingodb.client.Key;
import io.dingodb.client.OperationContext;
import io.dingodb.client.Record;
import io.dingodb.client.RouteTable;
import io.dingodb.sdk.common.KeyValue;
import io.dingodb.sdk.common.Range;
import io.dingodb.sdk.common.codec.KeyValueCodec;
import io.dingodb.sdk.common.table.Table;
import io.dingodb.sdk.common.utils.Any;
import io.dingodb.sdk.common.utils.ByteArrayUtils;
import io.dingodb.sdk.common.utils.ByteArrayUtils.ComparableByteArray;
import io.dingodb.sdk.common.utils.LinkedIterator;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.experimental.Delegate;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.NavigableSet;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static io.dingodb.sdk.common.utils.Any.wrap;

public class ScanOperation implements Operation {

    @Getter
    @AllArgsConstructor
    public static class KeyScan {
        private final Key start;
        private final Key end;
        private final boolean withStart;
        private final boolean withEnd;
    }

    @Getter
    public static class Scan {
        @Delegate
        private final Range range;
        private final boolean withStart;
        private final boolean withEnd;

        public Scan(byte[] startKey, byte[] endKey, boolean withStart, boolean withEnd) {
            this.range = new Range(startKey, endKey);
            this.withStart = withStart;
            this.withEnd = withEnd;
        }

        public Scan(Range range, boolean withStart, boolean withEnd) {
            this.range = range;
            this.withStart = withStart;
            this.withEnd = withEnd;
        }
    }

    private static final ScanOperation INSTANCE = new ScanOperation();

    private ScanOperation() {
    }

    public static ScanOperation getInstance() {
        return INSTANCE;
    }

    @Override
    public Operation.Fork fork(Any parameters, Table table, RouteTable routeTable) {
        try {
            KeyValueCodec codec = routeTable.getCodec();
            KeyScan keyScan = parameters.getValue();
            Scan scan = new Scan(
                codec.encodeKey(keyScan.start.getUserKey().toArray(new Object[table.getColumns().size()])),
                codec.encodeKey(keyScan.end.getUserKey().toArray(new Object[table.getColumns().size()])),
                keyScan.withStart,
                keyScan.withEnd
            );
            NavigableSet<Task> subTasks = routeTable.getRangeDistribution()
                .subMap(
                    new ComparableByteArray(scan.getRange().getStartKey()), scan.isWithStart(),
                    new ComparableByteArray(scan.getRange().getEndKey()), scan.isWithEnd()
                ).values().stream()
                .map(rd -> new Task(
                    rd.getId(),
                    wrap(new Scan(rd.getRange().getStartKey(), rd.getRange().getEndKey(), true, false)))
                ).collect(Collectors.toCollection(() -> new TreeSet<>(getComparator())));
            Task task = subTasks.pollFirst();
            if (task == null) {
                return new Fork(new Iterator[0], subTasks, true);
            }
            Scan taskScan = task.parameters();
            subTasks.add(new Task(
                task.getRegionId(),
                wrap(new Scan(scan.getStartKey(), taskScan.getEndKey(), scan.withStart, taskScan.withEnd))
            ));
            task = subTasks.pollLast();
            subTasks.add(new Task(
                task.getRegionId(),
                wrap(new Scan(taskScan.getStartKey(), scan.getEndKey(), taskScan.withStart, scan.withEnd))
            ));
            return new Fork(new Iterator[subTasks.size()], subTasks, true);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private Comparator<Task> getComparator() {
        return (e1, e2) -> ByteArrayUtils.compare(e1.<Scan>parameters().getStartKey(), e2.<Scan>parameters().getStartKey());
    }

    @Override
    public void exec(OperationContext context) {
        Scan scan = context.parameters();

        Iterator<KeyValue> scanResult = context.getStoreService()
            .scan(context.getTableId(), context.getRegionId(), scan.range, scan.withStart, scan.withEnd);

        context.<Iterator<Record>[]>result()[context.getSeq()] = new RecordIterator(
            context.getTable().getColumns(), context.getCodec(), scanResult
        );
    }

    @Override
    public <R> R reduce(Fork fork) {
        LinkedIterator<Record> result = new LinkedIterator<>();
        Arrays.stream(fork.<Iterator<Record>[]>result()).forEach(result::append);
        return (R) result;
    }
}
