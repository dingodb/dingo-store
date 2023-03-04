// Copyright (c) 2023 dingodb.com, Inc. All Rights Reserved
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "engine/storage.h"

namespace dingodb {

Storage::Storage(std::shared_ptr<Engine> engine) : engine_(engine) {}

Storage::~Storage() {}

Snapshot* Storage::GetSnapshot() { return nullptr; }

void Storage::ReleaseSnapshot() {}

pb::error::Errno Storage::KvGet(std::shared_ptr<Context> ctx,
                                const std::string& key, std::string& value) {
  return engine_->KvGet(ctx, key, value);
}

pb::error::Errno Storage::KvBatchGet(std::shared_ptr<Context> ctx,
                                     const std::vector<std::string>& keys,
                                     std::vector<pb::common::KeyValue>& kvs) {
  return engine_->KvBatchGet(ctx, keys, kvs);
}

pb::error::Errno Storage::KvPut(std::shared_ptr<Context> ctx,
                                const pb::common::KeyValue& kv) {
  return engine_->KvPut(ctx, kv);
}

pb::error::Errno Storage::KvBatchPut(
    std::shared_ptr<Context> ctx,
    const std::vector<pb::common::KeyValue>& kvs) {
  return engine_->KvBatchPut(ctx, kvs);
}

pb::error::Errno Storage::KvPutIfAbsent(std::shared_ptr<Context> ctx,
                                        const pb::common::KeyValue& kv) {
  return engine_->KvPutIfAbsent(ctx, kv);
}

pb::error::Errno Storage::KvBatchPutIfAbsent(
    std::shared_ptr<Context> ctx, const std::vector<pb::common::KeyValue>& kvs,
    std::vector<std::string>& put_keys) {
  return engine_->KvBatchPutIfAbsent(ctx, kvs, put_keys);
}

}  // namespace dingodb