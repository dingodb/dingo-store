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
#include <optional>
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
      : id(id), apply_log_index(0), snapshot_log_index(0), vector_index_parameter(vector_index_parameter) {
    vector_index_type = vector_index_parameter.vector_index_type();
  }

  virtual ~VectorIndex();

  VectorIndex(const VectorIndex& rhs) = delete;
  VectorIndex& operator=(const VectorIndex& rhs) = delete;
  VectorIndex(VectorIndex&& rhs) = delete;
  VectorIndex& operator=(VectorIndex&& rhs) = delete;

  pb::common::VectorIndexType VectorIndexType() const;

  virtual butil::Status Add(const std::vector<pb::common::VectorWithId>& vector_with_ids);
  virtual butil::Status Add(const pb::common::VectorWithId& vector_with_id);
  virtual butil::Status Add([[maybe_unused]] uint64_t id, [[maybe_unused]] const std::vector<float>& vector) = 0;

  virtual butil::Status Upsert(const std::vector<pb::common::VectorWithId>& vector_with_ids);
  virtual butil::Status Upsert(const pb::common::VectorWithId& vector_with_id);
  virtual butil::Status Upsert([[maybe_unused]] uint64_t id, [[maybe_unused]] const std::vector<float>& vector) = 0;

  virtual butil::Status Delete([[maybe_unused]] uint64_t id) = 0;

  virtual butil::Status Save([[maybe_unused]] const std::string& path);

  virtual butil::Status Load([[maybe_unused]] const std::string& path);

  virtual butil::Status Search([[maybe_unused]] pb::common::VectorWithId vector_with_id, [[maybe_unused]] uint32_t topk,
                               std::vector<pb::common::VectorWithDistance>& results,
                               [[maybe_unused]] bool reconstruct = false);

  virtual butil::Status Search([[maybe_unused]] const std::vector<float>& vector, [[maybe_unused]] uint32_t topk,
                               std::vector<pb::common::VectorWithDistance>& /*results*/,
                               [[maybe_unused]] bool reconstruct = false) = 0;

  virtual butil::Status SetOnline() = 0;

  virtual butil::Status SetOffline() = 0;

  uint64_t Id() const { return id; }

  uint64_t ApplyLogIndex() const;

  void SetApplyLogIndex(uint64_t apply_log_index);

  uint64_t SnapshotLogIndex() const;

  void SetSnapshotLogIndex(uint64_t snapshot_log_index);

 protected:
  // region_id
  uint64_t id;

  // apply max log index
  std::atomic<uint64_t> apply_log_index;

  // last snapshot log index
  std::atomic<uint64_t> snapshot_log_index;

  pb::common::VectorIndexType vector_index_type;

  pb::common::VectorIndexParameter vector_index_parameter;
};

}  // namespace dingodb

#endif  // DINGODB_VECTOR_INDEX_H_  // NOLINT
