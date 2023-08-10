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
import io.dingodb.common.Common;
import io.dingodb.common.Common.RangeWithOptions;
import io.dingodb.sdk.common.DingoClientException;
import io.dingodb.sdk.common.DingoCommonId;
import io.dingodb.sdk.common.KeyValue;
import io.dingodb.sdk.common.utils.EntityConversion;
import io.dingodb.sdk.service.connector.StoreServiceConnector;
import io.dingodb.store.Store;
import io.dingodb.store.StoreServiceGrpc;

import java.util.Collections;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicReference;

public class ScanIterator implements Iterator<KeyValue>, AutoCloseable {

    private final AtomicReference<StoreServiceGrpc.StoreServiceBlockingStub> stub = new AtomicReference<>();
    private final StoreServiceConnector connector;
    private final DingoCommonId regionId;
    private final RangeWithOptions range;

    private final ByteString scanId;
    private final int retryTimes;

    private final Coprocessor coprocessor;

    private Iterator<KeyValue> delegateIterator = Collections.<KeyValue>emptyList().iterator();
    private boolean release = false;

    public ScanIterator(
        StoreServiceConnector connector,
        DingoCommonId regionId,
        RangeWithOptions range,
        boolean key_only,
        int retryTimes,
        Coprocessor coprocessor
    ) {
        this.connector = connector;
        this.regionId = regionId;
        this.range = range;
        this.retryTimes = retryTimes;
        this.coprocessor = coprocessor;
        this.scanId = scanBegin();
        if (scanId == null || scanId.isEmpty()) {
            release = true;
        }
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
        Store.KvScanBeginRequest.Builder builder = Store.KvScanBeginRequest.newBuilder()
                .setRange(range)
                .setRegionId(regionId.entityId())
                .setMaxFetchCnt(0);
        if (coprocessor != null) {
            builder.setCoprocessor(EntityConversion.mapping(coprocessor, regionId));
        }
        Store.KvScanBeginRequest request = builder.build();
        Store.KvScanBeginResponse response = connector.exec(stub -> {
            this.stub.set(stub);
            return stub.kvScanBegin(request);
        });
        return response.getScanId();
    }

    public synchronized void scanContinue() {
        if (delegateIterator.hasNext()) {
            return;
        }
        Store.KvScanContinueResponse response = stub.get().kvScanContinue(Store.KvScanContinueRequest.newBuilder()
            .setScanId(scanId)
            .setRegionId(regionId.entityId())
            .setMaxFetchCnt(10)
            .build());
        checkRes(response.getError(), "continue");
        delegateIterator = response.getKvsList().stream().map(this::mapping).iterator();
        if (!delegateIterator.hasNext()) {
            release = true;
        }
    }


    public void scanRelease() {
        Store.KvScanReleaseResponse response = stub.get().kvScanRelease(Store.KvScanReleaseRequest.newBuilder()
            .setRegionId(regionId.entityId())
            .setScanId(scanId)
            .build());
        checkRes(response.getError(), "release");
    }

    @Override
    public synchronized void close() {
        if (release) {
            return;
        }
        scanRelease();
    }

    @Override
    protected void finalize() throws Throwable {
        close();
        super.finalize();
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
