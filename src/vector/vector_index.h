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
#include "proto/index.pb.h"

namespace dingodb {

class VectorIndex {
 public:
  VectorIndex(uint64_t id, const pb::common::VectorIndexParameter& vector_index_parameter)
      : id(id),
        status(pb::common::VECTOR_INDEX_STATUS_NONE),
        apply_log_index(0),
        snapshot_log_index(0),
        vector_index_parameter(vector_index_parameter) {
    vector_index_type = vector_index_parameter.vector_index_type();
  }

  virtual ~VectorIndex();

  VectorIndex(const VectorIndex& rhs) = delete;
  VectorIndex& operator=(const VectorIndex& rhs) = delete;
  VectorIndex(VectorIndex&& rhs) = delete;
  VectorIndex& operator=(VectorIndex&& rhs) = delete;

  pb::common::VectorIndexType VectorIndexType() const;

  virtual butil::Status GetCount([[maybe_unused]] uint64_t& count);

  virtual butil::Status NeedToRebuild([[maybe_unused]] bool& need_to_rebuild,
                                      [[maybe_unused]] uint64_t last_save_log_behind);
  virtual butil::Status NeedToSave([[maybe_unused]] bool& need_to_save, [[maybe_unused]] uint64_t last_save_log_behind);

  virtual butil::Status Add(const std::vector<pb::common::VectorWithId>& vector_with_ids) = 0;

  virtual butil::Status Upsert(const std::vector<pb::common::VectorWithId>& vector_with_ids) = 0;

  virtual butil::Status Delete([[maybe_unused]] const std::vector<uint64_t>& delete_ids) = 0;

  virtual butil::Status Save([[maybe_unused]] const std::string& path);

  virtual butil::Status Load([[maybe_unused]] const std::string& path);

  virtual butil::Status Search([[maybe_unused]] std::vector<pb::common::VectorWithId> vector_with_ids,
                                    [[maybe_unused]] uint32_t topk,
                                    std::vector<pb::index::VectorWithDistanceResult>& results,
                                    [[maybe_unused]] bool reconstruct = false) = 0;

  virtual butil::Status SetOnline() = 0;
  virtual butil::Status SetOffline() = 0;
  virtual bool IsOnline() = 0;

  virtual void LockWrite() = 0;
  virtual void UnlockWrite() = 0;

  uint64_t Id() const { return id; }

  pb::common::RegionVectorIndexStatus Status() { return status.load(); }

  void SetStatus(pb::common::RegionVectorIndexStatus status) { this->status.store(status); }

  uint64_t ApplyLogIndex() const;

  void SetApplyLogIndex(uint64_t apply_log_index);

  uint64_t SnapshotLogIndex() const;

  void SetSnapshotLogIndex(uint64_t snapshot_log_index);

 protected:
  // region_id
  uint64_t id;

  // status
  std::atomic<pb::common::RegionVectorIndexStatus> status;

  // apply max log index
  std::atomic<uint64_t> apply_log_index;

  // last snapshot log index
  std::atomic<uint64_t> snapshot_log_index;

  pb::common::VectorIndexType vector_index_type;

  pb::common::VectorIndexParameter vector_index_parameter;
};

}  // namespace dingodb

#endif  // DINGODB_VECTOR_INDEX_H_  // NOLINT
