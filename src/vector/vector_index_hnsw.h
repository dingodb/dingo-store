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

#ifndef DINGODB_VECTOR_INDEX_HNSW_H_  // NOLINT
#define DINGODB_VECTOR_INDEX_HNSW_H_

#include <atomic>
#include <cassert>
#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "bthread/types.h"
#include "butil/status.h"
#include "common/logging.h"
#include "hnswlib/space_ip.h"
#include "hnswlib/space_l2.h"
#include "proto/common.pb.h"
#include "proto/error.pb.h"
#include "vector/vector_index.h"

namespace dingodb {

class VectorIndexHnsw : public VectorIndex {
 public:
  explicit VectorIndexHnsw(uint64_t id, const pb::common::VectorIndexParameter& vector_index_parameter);

  ~VectorIndexHnsw() override;

  VectorIndexHnsw(const VectorIndexHnsw& rhs) = delete;
  VectorIndexHnsw& operator=(const VectorIndexHnsw& rhs) = delete;
  VectorIndexHnsw(VectorIndexHnsw&& rhs) = delete;
  VectorIndexHnsw& operator=(VectorIndexHnsw&& rhs) = delete;

  butil::Status Upsert(const std::vector<pb::common::VectorWithId>& vector_with_ids) override;
  butil::Status Add(const std::vector<pb::common::VectorWithId>& vector_with_ids) override;

  butil::Status Delete(const std::vector<uint64_t>& delete_ids) override;

  butil::Status Save(const std::string& path) override;
  butil::Status Load(const std::string& path) override;

  butil::Status SetOnline() override;
  butil::Status SetOffline() override;
  bool IsOnline() override;

  void LockWrite() override;
  void UnlockWrite() override;

  butil::Status Search(std::vector<pb::common::VectorWithId> vector_with_ids, uint32_t topk,
                       std::vector<pb::index::VectorWithDistanceResult>& results, bool reconstruct = false) override;

  butil::Status GetCount([[maybe_unused]] uint64_t& count) override;
  butil::Status NeedToRebuild([[maybe_unused]] bool& need_to_rebuild,
                              [[maybe_unused]] uint64_t last_save_log_behind) override;
  butil::Status NeedToSave([[maybe_unused]] bool& need_to_save,
                           [[maybe_unused]] uint64_t last_save_log_behind) override;

  butil::Status ResizeMaxElements(uint64_t new_max_elements);
  butil::Status GetMaxElements(uint64_t& max_elements);

 private:
  // hnsw members
  hnswlib::HierarchicalNSW<float>* hnsw_index_;
  hnswlib::SpaceInterface<float>* hnsw_space_;

  // hnsw parallel threads count
  uint32_t hnsw_num_threads_;

  // Dimension of the elements
  uint32_t dimension_;

  bthread_mutex_t mutex_;
  std::atomic<bool> is_online_;
};

}  // namespace dingodb

#endif  // DINGODB_VECTOR_INDEX_HNSW_H_
