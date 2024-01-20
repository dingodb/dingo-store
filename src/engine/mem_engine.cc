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

bool MemEngine::Init(std::shared_ptr<Config> /*config*/) {
  DINGO_LOG(INFO) << "Init MemEngine...";
  return true;
}

std::string MemEngine::GetName() { return pb::common::StorageEngine_Name(GetID()); }

pb::common::StorageEngine MemEngine::GetID() { return pb::common::StorageEngine::STORE_ENG_MEMORY; }

butil::Status MemEngine::Write(std::shared_ptr<Context> /*ctx*/, std::shared_ptr<WriteData> /*write_data*/) {
  return butil::Status();
}

butil::Status MemEngine::AsyncWrite(std::shared_ptr<Context> /*ctx*/, std::shared_ptr<WriteData> /*write_data*/) {
  return butil::Status();
}

butil::Status MemEngine::AsyncWrite(std::shared_ptr<Context> /*ctx*/, std::shared_ptr<WriteData> /*write_data*/,
                                    WriteCbFunc /*cb*/) {
  return butil::Status();
}

std::shared_ptr<Engine::Reader> MemEngine::NewReader(pb::common::RawEngine) { return nullptr; }

std::shared_ptr<Engine::Writer> MemEngine::NewWriter(pb::common::RawEngine) { return nullptr; }

std::shared_ptr<Engine::VectorReader> MemEngine::NewVectorReader(pb::common::RawEngine) { return nullptr; }

std::shared_ptr<Engine::TxnReader> MemEngine::NewTxnReader(pb::common::RawEngine) { return nullptr; }

std::shared_ptr<Engine::TxnWriter> MemEngine::NewTxnWriter(pb::common::RawEngine) { return nullptr; }

}  // namespace dingodb
