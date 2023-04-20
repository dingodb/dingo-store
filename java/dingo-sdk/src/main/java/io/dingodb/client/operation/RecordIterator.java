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

import io.dingodb.client.Record;
import io.dingodb.sdk.common.KeyValue;
import io.dingodb.sdk.common.codec.KeyValueCodec;
import io.dingodb.sdk.common.table.Column;
import lombok.Getter;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

@Getter
public class RecordIterator implements java.util.Iterator<Record> {

    private final List<Column> columns;
    private final KeyValueCodec codec;
    private final Iterator<KeyValue> kvIterator;

    public RecordIterator(List<Column> columns, KeyValueCodec codec, Iterator<KeyValue> kvIterator) {
        this.columns = columns;
        this.codec = codec;
        this.kvIterator = kvIterator;
    }

    @Override
    public boolean hasNext() {
        return kvIterator.hasNext();
    }

    @Override
    public Record next() {
        try {
            return new Record(columns, codec.decode(kvIterator.next()));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
