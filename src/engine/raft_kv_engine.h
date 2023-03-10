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
#include "proto/error.pb.h"
#include "proto/store.pb.h"
#include "raft/raft_node_manager.h"

namespace dingodb {

class RaftControlAble {
 public:
  virtual ~RaftControlAble() = default;

  virtual pb::error::Errno AddRegion([[maybe_unused]] std::shared_ptr<Context> ctx,
                                     std::shared_ptr<pb::common::Region> region) = 0;
  virtual pb::error::Errno DestroyRegion([[maybe_unused]] std::shared_ptr<Context> ctx, uint64_t region_id) = 0;
  virtual pb::error::Errno ChangeRegion([[maybe_unused]] std::shared_ptr<Context> ctx, uint64_t region_id,
                                        std::vector<pb::common::Peer> peers) = 0;

 protected:
  RaftControlAble() = default;
};

class RaftKvEngine : public Engine, public RaftControlAble {
 public:
  RaftKvEngine(std::shared_ptr<Engine> engine, MetaControl* meta_control_);
  ~RaftKvEngine() override;

  bool Init(std::shared_ptr<Config> config) override;
  bool Recover() override;

  std::string GetName() override;
  pb::common::Engine GetID() override;

  pb::error::Errno AddRegion(std::shared_ptr<Context> ctx, std::shared_ptr<pb::common::Region> region) override;
  pb::error::Errno ChangeRegion(std::shared_ptr<Context> ctx, uint64_t region_id,
                                std::vector<pb::common::Peer> peers) override;
  pb::error::Errno DestroyRegion(std::shared_ptr<Context> ctx, uint64_t region_id) override;

  pb::error::Errno KvGet(std::shared_ptr<Context> ctx, const std::string& key, std::string& value) override;
  pb::error::Errno KvBatchGet(std::shared_ptr<Context> ctx, const std::vector<std::string>& keys,
                              std::vector<pb::common::KeyValue>& kvs) override;

  pb::error::Errno KvPut(std::shared_ptr<Context> ctx, const pb::common::KeyValue& kv) override;
  // pb::error::Errno KvAsyncPut(std::shared_ptr<Context> ctx, const pb::common::KeyValue& kv) override;
  pb::error::Errno KvBatchPut(std::shared_ptr<Context> ctx, const std::vector<pb::common::KeyValue>& kvs) override;

  pb::error::Errno KvScan(std::shared_ptr<Context> ctx, const std::string& start_key, const std::string& end_key,
                          std::vector<pb::common::KeyValue>& kvs) override;

  pb::error::Errno KvPutIfAbsent(std::shared_ptr<Context> ctx, const pb::common::KeyValue& kv) override;
  pb::error::Errno KvBatchPutIfAbsentAtomic(std::shared_ptr<Context> ctx, const std::vector<pb::common::KeyValue>& kvs,
                                            std::vector<std::string>& put_keys) override;

  pb::error::Errno KvDeleteRange(std::shared_ptr<Context> ctx, const pb::common::Range& range) override;

  pb::error::Errno MetaPut(std::shared_ptr<Context> ctx, const pb::coordinator_internal::MetaIncrement& meta) override;

 private:
  MetaControl* meta_control_;
  std::shared_ptr<Engine> engine_;
  std::unique_ptr<RaftNodeManager> raft_node_manager_;
};

}  // namespace dingodb

#endif  // DINGODB_ENGINE_RAFT_KV_ENGINE_H_H  // NOLINT
