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

#include "common/logging.h"

namespace dingodb {

MemEngine::MemEngine() {}

bool MemEngine::Init(std::shared_ptr<Config> config) {
  DINGO_LOG(INFO) << "Init MemEngine...";
  return true;
}

std::string MemEngine::GetName() { return "MEM_ENGINE"; }

pb::common::Engine MemEngine::GetID() { return pb::common::ENG_MEMORY; }

butil::Status MemEngine::Write(std::shared_ptr<Context> ctx, const WriteData& write_data) { return butil::Status(); }
butil::Status MemEngine::AsyncWrite(std::shared_ptr<Context> ctx, const WriteData& write_data, WriteCb_t cb) {
  return butil::Status();
}

std::shared_ptr<Engine::Reader> MemEngine::NewReader(const std::string& cf_name) { return nullptr; }

}  // namespace dingodb
