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

#ifndef DINGODB_ENGINE_STORAGE_H_
#define DINGODB_ENGINE_STORAGE_H_

#include "common/context.h"
#include "engine/engine.h"
#include "engine/raft_kv_engine.h"
#include "memory"
#include "proto/common.pb.h"
#include "proto/error.pb.h"

namespace dingodb {

class Storage {
 public:
  Storage(std::shared_ptr<Engine> engine);
  ~Storage();

  int AddRegion(uint64_t region_id, const dingodb::pb::common::Region& region);
  int DestroyRegion(uint64_t region_id);

  Snapshot* GetSnapshot();
  void ReleaseSnapshot();

  std::shared_ptr<std::string> KvGet(std::shared_ptr<Context> ctx,
                                     const std::string& key);
  pb::error::Errno KvPut(std::shared_ptr<Context> ctx,
                         const pb::common::KeyValue& kv);

 private:
  std::shared_ptr<Engine> engine_;
};

}  // namespace dingodb

#endif  // DINGODB_ENGINE_STORAGE_H_