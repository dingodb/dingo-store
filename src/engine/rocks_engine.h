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

#ifndef DINGODB_ENGINE_ROCKS_ENGINE_H_  // NOLINT
#define DINGODB_ENGINE_ROCKS_ENGINE_H_

#include <cstdint>
#include <memory>
#include <string>

#include "config/config.h"
#include "engine/engine.h"

namespace dingodb {

class RocksEngine : public Engine {
 public:
  RocksEngine() = default;
  ~RocksEngine() override = default;

  RocksEngine(const RocksEngine& rhs) = delete;
  RocksEngine& operator=(const RocksEngine& rhs) = delete;
  RocksEngine(RocksEngine&& rhs) = delete;
  RocksEngine& operator=(RocksEngine&& rhs) = delete;

  bool Init(std::shared_ptr<Config> config) override;
  std::string GetName() override;
  pb::common::StorageEngine GetID() override;

  std::shared_ptr<Snapshot> GetSnapshot() override { return nullptr; }
  butil::Status SaveSnapshot(std::shared_ptr<Context>, int64_t, bool) override { return butil::Status(); }
  butil::Status AyncSaveSnapshot(std::shared_ptr<Context>, int64_t, bool) override { return butil::Status(); }

  butil::Status Write(std::shared_ptr<Context> ctx, std::shared_ptr<WriteData> write_data) override;
  butil::Status AsyncWrite(std::shared_ptr<Context> ctx, std::shared_ptr<WriteData> write_data) override;
  butil::Status AsyncWrite(std::shared_ptr<Context> ctx, std::shared_ptr<WriteData> write_data,
                           WriteCbFunc write_cb) override;

  std::shared_ptr<Engine::Reader> NewReader(pb::common::RawEngine type) override;
  std::shared_ptr<Engine::Writer> NewWriter(pb::common::RawEngine type) override;
  std::shared_ptr<Engine::VectorReader> NewVectorReader(pb::common::RawEngine type) override;
  std::shared_ptr<Engine::TxnReader> NewTxnReader(pb::common::RawEngine type) override;
  std::shared_ptr<Engine::TxnWriter> NewTxnWriter(pb::common::RawEngine type) override;
};

}  // namespace dingodb

#endif  // DINGODB_ENGINE_ROCKS_ENGINE_H_  // NOLINT
