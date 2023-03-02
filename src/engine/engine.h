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

#ifndef DINGODB_ENGINE_ENGINE_H_
#define DINGODB_ENGINE_ENGINE_H_

#include "common/context.h"
#include "config/config.h"
#include "engine/snapshot.h"
#include "proto/common.pb.h"
#include "proto/error.pb.h"

namespace dingodb {

class Engine {
 public:
  virtual ~Engine() = default;

  virtual bool Init(std::shared_ptr<Config> config) = 0;
  virtual std::string GetName() = 0;
  virtual pb::common::Engine GetID() = 0;

  virtual int AddRegion([[maybe_unused]] uint64_t region_id,
                        [[maybe_unused]] const pb::common::Region& region) {
    return -1;
  }
  virtual int DestroyRegion([[maybe_unused]] uint64_t region_id) { return -1; }

  virtual Snapshot* GetSnapshot() { return nullptr; }
  virtual void ReleaseSnapshot() {}

  virtual std::shared_ptr<std::string> KvGet(std::shared_ptr<Context> ctx,
                                             const std::string& key) = 0;
  virtual pb::error::Errno KvPut(std::shared_ptr<Context> ctx,
                                 const pb::common::KeyValue& kv) = 0;

 protected:
  Engine() = default;
  ;
};

}  // namespace dingodb

#endif  // DINGODB_ENGINE_ENGINE_H_