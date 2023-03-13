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

#ifndef DINGODB_ENGINE_RAFT_KV_ENGINE_H_
#define DINGODB_ENGINE_RAFT_KV_ENGINE_H_

#include <memory>

#include "engine/engine.h"
#include "engine/raw_engine.h"
#include "proto/error.pb.h"
#include "proto/store.pb.h"
#include "raft/raft_node_manager.h"

namespace dingodb {

class RaftControlAble {
 public:
  virtual ~RaftControlAble() = default;

  virtual butil::Status AddRegion([[maybe_unused]] std::shared_ptr<Context> ctx,
                                  std::shared_ptr<pb::common::Region> region) = 0;
  virtual butil::Status DestroyRegion([[maybe_unused]] std::shared_ptr<Context> ctx, uint64_t region_id) = 0;
  virtual butil::Status ChangeRegion([[maybe_unused]] std::shared_ptr<Context> ctx, uint64_t region_id,
                                     std::vector<pb::common::Peer> peers) = 0;

 protected:
  RaftControlAble() = default;
};

class RaftKvEngine : public Engine, public RaftControlAble {
 public:
  RaftKvEngine(std::shared_ptr<RawEngine> engine);
  ~RaftKvEngine() override;

  bool Init(std::shared_ptr<Config> config) override;
  bool Recover() override;

  std::string GetName() override;
  pb::common::Engine GetID() override;

  butil::Status AddRegion(std::shared_ptr<Context> ctx, std::shared_ptr<pb::common::Region> region) override;
  butil::Status ChangeRegion(std::shared_ptr<Context> ctx, uint64_t region_id,
                             std::vector<pb::common::Peer> peers) override;
  butil::Status DestroyRegion(std::shared_ptr<Context> ctx, uint64_t region_id) override;

  butil::Status Write(std::shared_ptr<Context> ctx, const WriteData& write_data) override;
  butil::Status AsyncWrite(std::shared_ptr<Context> ctx, const WriteData& write_data, WriteCb_t cb) override;

  std::shared_ptr<Engine::Reader> NewReader(const std::string& cf_name) override;

  class Reader : public Engine::Reader {
   public:
    Reader(std::shared_ptr<RawEngine::Reader> reader) : reader_(reader) {}
    butil::Status KvGet(std::shared_ptr<Context> ctx, const std::string& key, std::string& value) override;

    butil::Status KvScan(std::shared_ptr<Context> ctx, const std::string& start_key, const std::string& end_key,
                         std::vector<pb::common::KeyValue>& kvs) override;

    butil::Status KvCount(std::shared_ptr<Context> ctx, const std::string& start_key, const std::string& end_key,
                          int64_t& count) override;

   private:
    std::shared_ptr<RawEngine::Reader> reader_;
  };

 protected:
  std::shared_ptr<RawEngine> engine_;                   // NOLINT
  std::unique_ptr<RaftNodeManager> raft_node_manager_;  // NOLINT
};

}  // namespace dingodb

#endif  // DINGODB_ENGINE_RAFT_KV_ENGINE_H_H  // NOLINT
