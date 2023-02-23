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

bool MemEngine::Init() {}
std::string MemEngine::GetName() {
  return "MEM_ENGINE";
}

uint32_t MemEngine::GetID() {
  return Engine::Type::MEM_ENGINE;
}

int MemEngine::AddRegion(uint64_t region_id, const dingodb::pb::common::Region& region) {
  return 0;
}

int MemEngine::DestroyRegion(uint64_t region_id) {
  return 0;
}

std::shared_ptr<std::string> MemEngine::KvGet(std::shared_ptr<Context> ctx, const std::string& key) {
  auto it = store_.find(key);
  if (it == store_.end()) {
    return nullptr;
  }

  return std::make_shared<std::string>(it->second);
}

int MemEngine::KvPut(std::shared_ptr<Context> ctx, const std::string& key, const std::string& value) {
  std::unique_lock<std::shared_mutex> lock(mutex_);
  store_[key] = value;
  return 0;
}




} // namespace dingodb