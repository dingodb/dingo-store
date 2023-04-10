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

#include <climits>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <map>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>

#include "butil/strings/stringprintf.h"
#include "config/config_manager.h"
#include "engine/engine.h"
#include "proto/error.pb.h"
#include "rocksdb/advanced_options.h"
#include "rocksdb/cache.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/iterator.h"
#include "rocksdb/slice.h"
#include "rocksdb/table.h"
#include "rocksdb/utilities/transaction_db.h"
#include "rocksdb/write_batch.h"

namespace dingodb {

bool RocksEngine::Init([[maybe_unused]] std::shared_ptr<Config> config) { return true; }
std::string RocksEngine::GetName() { return pb::common::Engine_Name(pb::common::ENG_ROCKSDB); }

pb::common::Engine RocksEngine::GetID() { return pb::common::ENG_ROCKSDB; }

// not implement
std::shared_ptr<Snapshot> RocksEngine::GetSnapshot() { return nullptr; }

butil::Status RocksEngine::Write(std::shared_ptr<Context> /*ctx*/, const WriteData& /*write_data*/) {
  return butil::Status();
}
butil::Status RocksEngine::AsyncWrite(std::shared_ptr<Context> /*ctx*/, const WriteData& /*write_data*/,
                                      WriteCbFunc /*write_cb*/) {
  return butil::Status();
}

std::shared_ptr<Engine::Reader> RocksEngine::NewReader(const std::string& /*cf_name*/) { return nullptr; }

}  // namespace dingodb
