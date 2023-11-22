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

#include <any>
#include <cstdint>
#include <memory>
#include <shared_mutex>
#include <vector>

#include "common/logging.h"
#include "fmt/core.h"
#include "proto/common.pb.h"

namespace dingodb {

// Support kv transform ability.
// This is abstract class.
class TransformKvAble {
 public:
  TransformKvAble(const std::string& prefix) : prefix_(prefix){};
  virtual ~TransformKvAble() = default;

  std::string Prefix() { return prefix_; }
  virtual std::string GenKey(int64_t id) { return fmt::format("{}_{}", prefix_, id); }
  virtual std::string GenKey(int64_t id, int64_t job_id) { return fmt::format("{}_{}_{}", prefix_, id, job_id); }

  virtual int64_t ParseRegionId(const std::string& str) {
    if (str.size() <= prefix_.size()) {
      DINGO_LOG(ERROR) << "Parse region id failed, invalid str " << str;
      return 0;
    }

    std::string s(str.c_str() + prefix_.size() + 1);
    try {
      return std::stoll(s, nullptr, 10);
    } catch (std::invalid_argument& e) {
      DINGO_LOG(ERROR) << "string to int64_t failed: " << e.what();
    }

    return 0;
  }

  // Transform other format to kv.
  virtual std::shared_ptr<pb::common::KeyValue> TransformToKv(std::any /*obj*/) {
    DINGO_LOG(ERROR) << "Not support";
    return nullptr;
  }
  // Transform other format to kv with delta.
  virtual std::vector<std::shared_ptr<pb::common::KeyValue> > TransformToKvtWithDelta() {
    DINGO_LOG(ERROR) << "Not support";
    return {};
  }
  // Transform other format to kv with all.
  virtual std::vector<std::shared_ptr<pb::common::KeyValue> > TransformToKvWithAll() {
    DINGO_LOG(ERROR) << "Not support";
    return {};
  }

  // Transform kv to other format.
  virtual void TransformFromKv(const std::vector<pb::common::KeyValue>& kvs) = 0;

 private:
  const std::string prefix_;
};

}  // namespace dingodb

#endif  // DINGODB_TRANSFORM_KV_ABLE_H_