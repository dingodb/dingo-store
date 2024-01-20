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
#include "engine/rocks_engine.h"

#include <memory>
#include <string>

#include "engine/engine.h"

namespace dingodb {

bool RocksEngine::Init([[maybe_unused]] std::shared_ptr<Config> config) { return true; }
std::string RocksEngine::GetName() {
  return pb::common::StorageEngine_Name(pb::common::StorageEngine::STORE_ENG_ROCKSDB);
}

pb::common::StorageEngine RocksEngine::GetID() { return pb::common::StorageEngine::STORE_ENG_ROCKSDB; }

butil::Status RocksEngine::Write(std::shared_ptr<Context> /*ctx*/, std::shared_ptr<WriteData> /*write_data*/) {
  return butil::Status();
}

butil::Status RocksEngine::AsyncWrite(std::shared_ptr<Context> /*ctx*/, std::shared_ptr<WriteData> /*write_data*/) {
  return butil::Status();
}

butil::Status RocksEngine::AsyncWrite(std::shared_ptr<Context> /*ctx*/, std::shared_ptr<WriteData> /*write_data*/,
                                      WriteCbFunc /*write_cb*/) {
  return butil::Status();
}

std::shared_ptr<Engine::Reader> RocksEngine::NewReader(pb::common::RawEngine) { return nullptr; }

std::shared_ptr<Engine::Writer> RocksEngine::NewWriter(pb::common::RawEngine) { return nullptr; }

std::shared_ptr<Engine::VectorReader> RocksEngine::NewVectorReader(pb::common::RawEngine) { return nullptr; }

std::shared_ptr<Engine::TxnReader> RocksEngine::NewTxnReader(pb::common::RawEngine) { return nullptr; }

std::shared_ptr<Engine::TxnWriter> RocksEngine::NewTxnWriter(pb::common::RawEngine) { return nullptr; }

}  // namespace dingodb
