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

#ifndef DINGODB_ENGINE_RAFT_META_ENGINE_H_
#define DINGODB_ENGINE_RAFT_META_ENGINE_H_

#include "engine/raft_kv_engine.h"

namespace dingodb {
class RaftMetaEngine : public RaftKvEngine {
 public:
  RaftMetaEngine(std::shared_ptr<Engine> engine, std::shared_ptr<MetaControl> meta_control);
  ~RaftMetaEngine() override;

  bool Init(std::shared_ptr<Config> config) override;
  bool Recover() override;

  pb::error::Errno AddRegion(std::shared_ptr<Context> ctx, std::shared_ptr<pb::common::Region> region) override;

  pb::error::Errno MetaPut(std::shared_ptr<Context> ctx, const pb::coordinator_internal::MetaIncrement& meta) override;

 private:
  std::shared_ptr<MetaControl> meta_control_;
};

}  // namespace dingodb

#endif  // DINGODB_ENGINE_RAFT_META_ENGINE_H_H  // NOLINT