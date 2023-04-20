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


package io.dingodb.sdk.service.store;

import com.google.protobuf.ByteString;
import io.dingodb.DingoClient;
import io.dingodb.client.Key;
import io.dingodb.client.Record;
import io.dingodb.client.Value;
import io.dingodb.common.Common;
import io.dingodb.common.Common.RangeWithOptions;
import io.dingodb.sdk.common.DingoClientException;
import io.dingodb.sdk.common.KeyValue;
import io.dingodb.sdk.common.partition.PartitionDetailDefinition;
import io.dingodb.sdk.common.partition.PartitionRule;
import io.dingodb.sdk.common.table.ColumnDefinition;
import io.dingodb.sdk.common.table.TableDefinition;
import io.dingodb.store.Store;
import io.dingodb.store.StoreServiceGrpc.StoreServiceBlockingStub;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.NoSuchElementException;

public class ScanIterator implements Iterator<KeyValue>, AutoCloseable {

    private final StoreServiceBlockingStub stub;
    private final long regionId;
    private final RangeWithOptions range;

    private final ByteString scanId;

    private Iterator<KeyValue> delegateIterator = Collections.<KeyValue>emptyList().iterator();
    private boolean release = false;

    public ScanIterator(
        StoreServiceBlockingStub stub, long regionId, RangeWithOptions range, boolean key_only
    ) {
        this.stub = stub;
        this.regionId = regionId;
        this.range = range;
        this.scanId = scanBegin();
    }

    private static void checkRes(io.dingodb.error.ErrorOuterClass.Error error, String param) {
        if (error.getErrcodeValue() != 0) {
            throw new DingoClientException(error.getErrcodeValue(), "Scan " + param +" error: " + error.getErrmsg());
        }
    }

    private KeyValue mapping(Common.KeyValue pbKv) {
        return new KeyValue(pbKv.getKey().toByteArray(), pbKv.getValue().toByteArray());
    }

    public ByteString scanBegin() {
        Store.KvScanBeginResponse response = stub.kvScanBegin(Store.KvScanBeginRequest.newBuilder()
            .setRange(range)
            .setRegionId(regionId)
            .setMaxFetchCnt(0)
            .build());
        checkRes(response.getError(), "begin");
        return response.getScanId();
    }

    public synchronized void scanContinue() {
        if (delegateIterator.hasNext()) {
            return;
        }
        Store.KvScanContinueResponse response = stub.kvScanContinue(Store.KvScanContinueRequest.newBuilder()
            .setScanId(scanId)
            .setRegionId(regionId)
            .setMaxFetchCnt(10)
            .build());
        checkRes(response.getError(), "continue");
        delegateIterator = response.getKvsList().stream().map(this::mapping).iterator();
        if (!delegateIterator.hasNext()) {
            release = true;
        }
    }


    public void scanRelease() {
        Store.KvScanReleaseResponse response = stub.kvScanRelease(Store.KvScanReleaseRequest.newBuilder()
            .setRegionId(regionId)
            .setScanId(scanId)
            .build());
        checkRes(response.getError(), "release");
    }

    @Override
    public void close() {
        if (release) {
            return;
        }
        scanRelease();
    }

    @Override
    public boolean hasNext() {
        if (release) {
            return false;
        }
        if (delegateIterator.hasNext()) {
            return true;
        }
        scanContinue();
        return delegateIterator.hasNext();
    }

    @Override
    public KeyValue next() {
        if (release) {
            throw new NoSuchElementException();
        }
        return delegateIterator.next();
    }

}
