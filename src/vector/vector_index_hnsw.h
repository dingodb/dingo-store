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

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "butil/status.h"
#include "common/synchronization.h"
#include "hnswlib/hnswlib.h"
#include "proto/common.pb.h"
#include "vector/vector_index.h"

namespace dingodb {

class VectorIndexHnsw : public VectorIndex {
 public:
  explicit VectorIndexHnsw(int64_t id, const pb::common::VectorIndexParameter& vector_index_parameter,
                           const pb::common::RegionEpoch& epoch, const pb::common::Range& range,
                           ThreadPoolPtr thread_pool);

  ~VectorIndexHnsw() override;

  static uint32_t CalcHnswCountFromMemory(int64_t memory_size_limit, int64_t dimension, int64_t nlinks);
  static butil::Status CheckAndSetHnswParameter(pb::common::CreateHnswParam& hnsw_parameter);

  VectorIndexHnsw(const VectorIndexHnsw& rhs) = delete;
  VectorIndexHnsw& operator=(const VectorIndexHnsw& rhs) = delete;
  VectorIndexHnsw(VectorIndexHnsw&& rhs) = delete;
  VectorIndexHnsw& operator=(VectorIndexHnsw&& rhs) = delete;

  butil::Status Add(const std::vector<pb::common::VectorWithId>& vector_with_ids) override;
  butil::Status Add(const std::vector<pb::common::VectorWithId>& vector_with_ids, bool is_priority) override;

  butil::Status Upsert(const std::vector<pb::common::VectorWithId>& vector_with_ids) override;
  butil::Status Upsert(const std::vector<pb::common::VectorWithId>& vector_with_ids, bool is_priority) override;

  butil::Status Delete(const std::vector<int64_t>& delete_ids) override;
  butil::Status Delete(const std::vector<int64_t>& delete_ids, bool is_priority) override;

  butil::Status Save(const std::string& path) override;
  butil::Status Load(const std::string& path) override;

  void LockWrite() override;
  void UnlockWrite() override;

  butil::Status Search(const std::vector<pb::common::VectorWithId>& vector_with_ids, uint32_t topk,
                       const std::vector<std::shared_ptr<FilterFunctor>>& filters, bool reconstruct,
                       const pb::common::VectorSearchParameter& parameter,
                       std::vector<pb::index::VectorWithDistanceResult>& results) override;

  butil::Status RangeSearch(const std::vector<pb::common::VectorWithId>& vector_with_ids, float radius,
                            const std::vector<std::shared_ptr<VectorIndex::FilterFunctor>>& filters, bool reconstruct,
                            const pb::common::VectorSearchParameter& parameter,
                            std::vector<pb::index::VectorWithDistanceResult>& results) override;

  int32_t GetDimension() override;
  pb::common::MetricType GetMetricType() override;
  butil::Status GetCount(int64_t& count) override;
  butil::Status GetDeletedCount(int64_t& deleted_count) override;
  butil::Status GetMemorySize(int64_t& memory_size) override;

  butil::Status ResizeMaxElements(int64_t new_max_elements);
  butil::Status GetMaxElements(int64_t& max_elements);

  bool IsExceedsMaxElements() override;

  butil::Status Train([[maybe_unused]] const std::vector<float>& train_datas) override { return butil::Status::OK(); }
  butil::Status Train([[maybe_unused]] const std::vector<pb::common::VectorWithId>& vectors) override {
    return butil::Status::OK();
  }
  bool NeedToRebuild() override;
  bool NeedToSave(int64_t last_save_log_behind) override;
  bool SupportSave() override;

  hnswlib::HierarchicalNSW<float>* GetHnswIndex();

  // void NormalizeVector(const float* data, float* norm_array) const;

 private:
  // hnsw members
  hnswlib::HierarchicalNSW<float>* hnsw_index_;
  hnswlib::SpaceInterface<float>* hnsw_space_;

  // Dimension of the elements
  uint32_t dimension_;

  // bthread_mutex_t mutex_;
  RWLock rw_lock_;

  uint32_t max_element_limit_;

  // normalize vector
  bool normalize_;
};

}  // namespace dingodb

#endif  // DINGODB_VECTOR_INDEX_HNSW_H_
