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

#ifndef DINGODB_TRANSFORM_KV_ABLE_H_
#define DINGODB_TRANSFORM_KV_ABLE_H_

#include <memory>
#include <shared_mutex>
#include <vector>

#include "butil/endpoint.h"
#include "engine/engine.h"
#include "meta/meta_reader.h"
#include "meta/meta_writer.h"
#include "proto/common.pb.h"

namespace dingodb {

// Support kv transform ability.
// This is abstract class.
class TransformKvAble {
 public:
  TransformKvAble(const std::string& prefix) : prefix_(prefix){};
  virtual ~TransformKvAble() = default;

  std::string prefix() { return prefix_; }
  virtual std::string GenKey(uint64_t /*region_id*/) { return ""; }

  // Transform other format to kv.
  virtual std::shared_ptr<pb::common::KeyValue> TransformToKv(uint64_t id) = 0;
  virtual std::shared_ptr<pb::common::KeyValue> TransformToKv(const std::shared_ptr<pb::common::Region> region) = 0;
  // Transform other format to kv with delta.
  virtual std::vector<std::shared_ptr<pb::common::KeyValue> > TransformToKvtWithDelta() = 0;
  // Transform other format to kv with all.
  virtual std::vector<std::shared_ptr<pb::common::KeyValue> > TransformToKvWithAll() = 0;

  // Transform kv to other format.
  virtual void TransformFromKv(const std::vector<pb::common::KeyValue>& kvs) = 0;

 protected:
  const std::string prefix_;
};

}  // namespace dingodb

#endif  // DINGODB_TRANSFORM_KV_ABLE_H_