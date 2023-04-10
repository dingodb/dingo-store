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

package io.dingodb;

import io.dingodb.sdk.service.meta.MetaClient;

public class UnifyStoreConnection {

    private MetaClient metaClient;
    private Integer retryTimes;
    private String schema;

    public UnifyStoreConnection(String coordinatorSvr, String schema, Integer retryTimes) {
        this.metaClient = new MetaClient(coordinatorSvr);
        this.retryTimes = retryTimes;
        this.schema = schema;
    }

    public void initConnection() {
        this.metaClient.init(schema);
    }

    public MetaClient getMetaClient() {
        return metaClient;
    }
}
