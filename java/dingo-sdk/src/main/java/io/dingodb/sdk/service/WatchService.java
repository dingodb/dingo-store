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

package io.dingodb.sdk.service;

import io.dingodb.sdk.common.DingoClientException;
import io.dingodb.sdk.service.entity.common.Location;
import io.dingodb.sdk.service.entity.version.Event;
import io.dingodb.sdk.service.entity.version.Kv;
import io.dingodb.sdk.service.entity.version.WatchRequest;
import lombok.extern.slf4j.Slf4j;

import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import static io.dingodb.sdk.service.LockService.LOCK_FUTURE_POOL;
import static io.dingodb.sdk.service.entity.version.EventType.DELETE;
import static io.dingodb.sdk.service.entity.version.EventType.NOT_EXISTS;

@Slf4j
public class WatchService {
    protected Set<Location> locations;
    protected VersionService kvService;

    public WatchService(String servers) {
        this.locations = Services.parse(servers);
        this.kvService = Services.versionService(locations);
    }

    public void watchAllOpEvent(Kv kv, Function<String, String> function) {
        CompletableFuture.supplyAsync(() ->
            kvService.watch(watchAllOpRequest(kv.getKv().getKey(), kv.getModRevision()))
        ).whenCompleteAsync((r, e) -> {
            if (e != null) {
                log.error("Watch locked error, or watch retry time great than lease ttl.", e);
                if (!(e instanceof DingoClientException)) {
                    resetVerService();
                    watchAllOpEvent(kv, function);
                    return;
                }
                return;
            }
            String typeStr = "normal";
            if (r.getEvents() == null) {
                typeStr = "transferLeader";
            } else if (r.getEvents().stream().map(Event::getType).anyMatch(type -> type == DELETE || type == NOT_EXISTS)) {
                typeStr = "keyNone";
            }
            function.apply(typeStr);
            watchAllOpEvent(kv, function);
        }, LOCK_FUTURE_POOL);
    }

    private WatchRequest watchAllOpRequest(byte[] resourceKey, long revision) {
        return WatchRequest.builder()
            .requestUnion(WatchRequest.RequestUnionNest.OneTimeRequest.builder()
                .key(resourceKey)
                .needPrevKv(true)
                .startRevision(revision)
                .build()
            ).build();
    }

    public void resetVerService() {
        Services.invalidateVersionService(locations);
        kvService = Services.versionService(locations);
    }
}
