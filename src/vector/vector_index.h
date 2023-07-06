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

#ifndef DINGODB_VECTOR_INDEX_H_
#define DINGODB_VECTOR_INDEX_H_

#include <atomic>
#include <cassert>
#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "butil/status.h"
#include "common/logging.h"
#include "hnswlib/space_ip.h"
#include "hnswlib/space_l2.h"
#include "proto/common.pb.h"
#include "proto/error.pb.h"

namespace dingodb {

class VectorIndex {
 public:
  VectorIndex(uint64_t id, const pb::common::VectorIndexParameter& vector_index_parameter)
      : id_(id), apply_log_index_(0), snapshot_log_index_(0), vector_index_parameter_(vector_index_parameter) {
    vector_index_type_ = vector_index_parameter_.vector_index_type();
  }

  virtual ~VectorIndex() {}

  VectorIndex(const VectorIndex& rhs) = delete;
  VectorIndex& operator=(const VectorIndex& rhs) = delete;
  VectorIndex(VectorIndex&& rhs) = delete;
  VectorIndex& operator=(VectorIndex&& rhs) = delete;

  pb::common::VectorIndexType VectorIndexType() const { return vector_index_type_; }

  butil::Status Add(const std::vector<pb::common::VectorWithId>& vector_with_ids) {
    for (const auto& vector_with_id : vector_with_ids) {
      auto ret = Add(vector_with_id);
      if (!ret.ok()) {
        return ret;
      }
    }
    return butil::Status::OK();
  }

  butil::Status Add(const pb::common::VectorWithId& vector_with_id) {
    std::vector<float> vector;
    for (const auto& value : vector_with_id.vector().float_values()) {
      vector.push_back(value);
    }

    return Add(vector_with_id.id(), vector);
  }

  virtual butil::Status Add([[maybe_unused]] uint64_t id, [[maybe_unused]] const std::vector<float>& vector) {
    return butil::Status(pb::error::Errno::EVECTOR_NOT_SUPPORT, "Not Support");
  }

  virtual void Delete(uint64_t id) {}

  virtual butil::Status Save([[maybe_unused]] const std::string& path) {
    return butil::Status(pb::error::Errno::EVECTOR_NOT_SUPPORT, "Not Support");
  }

  virtual butil::Status Load([[maybe_unused]] const std::string& path) {
    return butil::Status(pb::error::Errno::EVECTOR_NOT_SUPPORT, "Not Support");
  }

  virtual butil::Status Search([[maybe_unused]] const std::vector<float>& vector, [[maybe_unused]] uint32_t topk,
                               std::vector<pb::common::VectorWithDistance>& results) {
    return butil::Status(pb::error::Errno::EVECTOR_NOT_SUPPORT, "Not Support");
  }

  virtual butil::Status Search([[maybe_unused]] pb::common::VectorWithId vector_with_id, [[maybe_unused]] uint32_t topk,
                               std::vector<pb::common::VectorWithDistance>& results) {
    return butil::Status(pb::error::Errno::EVECTOR_NOT_SUPPORT, "Not Support");
  }

  uint64_t Id() const { return id_; }

  uint64_t ApplyLogIndex() const { return apply_log_index_.load(std::memory_order_relaxed); }

  void SetApplyLogIndex(uint64_t apply_log_index) {
    apply_log_index_.store(apply_log_index, std::memory_order_relaxed);
  }

  uint64_t SnapshotLogIndex() const { return snapshot_log_index_.load(std::memory_order_relaxed); }

  void SetSnapshotLogIndex(uint64_t snapshot_log_index) {
    snapshot_log_index_.store(snapshot_log_index, std::memory_order_relaxed);
  }

 protected:
  // region_id
  uint64_t id_;

  // apply max log index
  std::atomic<uint64_t> apply_log_index_;

  // last snapshot log index
  std::atomic<uint64_t> snapshot_log_index_;

  pb::common::VectorIndexType vector_index_type_;

  pb::common::VectorIndexParameter vector_index_parameter_;
};

}  // namespace dingodb

#endif  // DINGODB_VECTOR_INDEX_H_  // NOLINT
