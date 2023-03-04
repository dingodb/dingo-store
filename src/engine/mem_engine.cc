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

#include "engine/mem_engine.h"

namespace dingodb {

MemEngine::MemEngine() {}

bool MemEngine::Init(std::shared_ptr<Config> config) {
  LOG(INFO) << "Init MemEngine...";
  return true;
}

std::string MemEngine::GetName() { return "MEM_ENGINE"; }

pb::common::Engine MemEngine::GetID() { return pb::common::ENG_MEMORY; }

pb::error::Errno MemEngine::KvGet(std::shared_ptr<Context> ctx,
                                  const std::string& key, std::string& value) {
  auto it = store_.find(key);
  if (it == store_.end()) {
    return pb::error::EKEY_NOTFOUND;
  }

  value = it->second;
  return pb::error::OK;
}

pb::error::Errno MemEngine::KvBatchGet(std::shared_ptr<Context> ctx,
                                       const std::vector<std::string>& keys,
                                       std::vector<pb::common::KeyValue>& kvs) {
  return pb::error::ENOT_SUPPORT;
}

pb::error::Errno MemEngine::KvPut(std::shared_ptr<Context> ctx,
                                  const pb::common::KeyValue& kv) {
  LOG(INFO) << "MemEngine::KvPut: " << kv.key() << " : " << kv.value();
  std::unique_lock<std::shared_mutex> lock(mutex_);
  store_[kv.key()] = kv.value();
  return pb::error::OK;
}

pb::error::Errno MemEngine::KvBatchPut(
    std::shared_ptr<Context> ctx,
    const std::vector<pb::common::KeyValue>& kvs) {
  return pb::error::ENOT_SUPPORT;
}

pb::error::Errno MemEngine::KvPutIfAbsent(std::shared_ptr<Context> ctx,
                                          const pb::common::KeyValue& kv) {
  return pb::error::ENOT_SUPPORT;
}

pb::error::Errno MemEngine::KvBatchPutIfAbsent(
    std::shared_ptr<Context> ctx, const std::vector<pb::common::KeyValue>& kvs,
    std::vector<std::string>& put_keys) {
  return pb::error::ENOT_SUPPORT;
}

}  // namespace dingodb
