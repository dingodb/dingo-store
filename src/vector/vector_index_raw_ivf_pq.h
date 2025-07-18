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

#ifndef DINGODB_VECTOR_INDEX_RAW_IVF_PQ_H_  // NOLINT
#define DINGODB_VECTOR_INDEX_RAW_IVF_PQ_H_

#include <faiss/IndexIVFPQ.h>

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "butil/status.h"
#include "faiss/Index.h"
#include "faiss/MetricType.h"
#include "faiss/impl/IDSelector.h"
#include "faiss/utils/distances.h"
#include "proto/common.pb.h"
#include "vector/vector_index.h"

namespace dingodb {

// Filter vector id
class RawIvfPqIDSelector : public faiss::IDSelector {
 public:
  explicit RawIvfPqIDSelector(std::vector<std::shared_ptr<VectorIndex::FilterFunctor>> filters) : filters_(filters) {}
  ~RawIvfPqIDSelector() override = default;
  bool is_member(faiss::idx_t id) const override {
    if (filters_.empty()) {
      return true;
    }
    for (const auto& filter : filters_) {
      if (!filter->Check(id)) {
        return false;
      }
    }

    return true;
  }

 private:
  std::vector<std::shared_ptr<VectorIndex::FilterFunctor>> filters_;
};

class VectorIndexRawIvfPq : public VectorIndex {
 public:
  explicit VectorIndexRawIvfPq(int64_t id, const pb::common::VectorIndexParameter& vector_index_parameter,
                               const pb::common::RegionEpoch& epoch, const pb::common::Range& range,
                               ThreadPoolPtr thread_pool);

  ~VectorIndexRawIvfPq() override;

  VectorIndexRawIvfPq(const VectorIndexRawIvfPq& rhs) = delete;
  VectorIndexRawIvfPq& operator=(const VectorIndexRawIvfPq& rhs) = delete;
  VectorIndexRawIvfPq(VectorIndexRawIvfPq&& rhs) = delete;
  VectorIndexRawIvfPq& operator=(VectorIndexRawIvfPq&& rhs) = delete;

  butil::Status Save(const std::string& path) override;
  butil::Status Load(const std::string& path) override;
  bool SupportSave() override;

  // in FLAT index, add two vector with same id will cause data conflict
  butil::Status Add(const std::vector<pb::common::VectorWithId>& vector_with_ids) override;

  butil::Status AddOrUpsertWrapper(const std::vector<pb::common::VectorWithId>& vector_with_ids, bool is_upsert);

  // not exist add. if exist update
  butil::Status Upsert(const std::vector<pb::common::VectorWithId>& vector_with_ids) override;

  butil::Status Delete(const std::vector<int64_t>& delete_ids) override;

  butil::Status Search(const std::vector<pb::common::VectorWithId>& vector_with_ids, uint32_t topk,
                       const std::vector<std::shared_ptr<FilterFunctor>>& filters, bool reconstruct,
                       const pb::common::VectorSearchParameter& parameter,
                       std::vector<pb::index::VectorWithDistanceResult>& results) override;

  butil::Status RangeSearch(const std::vector<pb::common::VectorWithId>& vector_with_ids, float radius,
                            const std::vector<std::shared_ptr<VectorIndex::FilterFunctor>>& filters, bool reconstruct,
                            const pb::common::VectorSearchParameter& parameter,
                            std::vector<pb::index::VectorWithDistanceResult>& results) override;

  void LockWrite() override;
  void UnlockWrite() override;

  int32_t GetDimension() override;
  pb::common::MetricType GetMetricType() override;
  butil::Status GetCount([[maybe_unused]] int64_t& count) override;
  butil::Status GetDeletedCount([[maybe_unused]] int64_t& deleted_count) override;
  butil::Status GetMemorySize([[maybe_unused]] int64_t& memory_size) override;
  bool IsExceedsMaxElements(int64_t vector_size) override;

  butil::Status Train(std::vector<float>& train_datas) override;
  butil::Status Train(const std::vector<pb::common::VectorWithId>& vectors) override;
  bool NeedToRebuild() override;
  bool NeedTrain() override { return true; }
  bool IsTrained() override;
  bool NeedToSave(int64_t last_save_log_behind) override;

 private:
  void Init();

  bool IsTrainedImpl();

  // train failed. reset
  void Reset();

  butil::Status AddOrUpsert(const std::vector<pb::common::VectorWithId>& vector_with_ids, bool is_upsert);

  // Dimension of the elements
  faiss::idx_t dimension_;

  // only support L2 and IP
  pb::common::MetricType metric_type_;

  RWLock rw_lock_;

  size_t nlist_;

  size_t nsubvector_;

  int32_t nbits_per_idx_;

  std::unique_ptr<faiss::Index> quantizer_;

  std::unique_ptr<faiss::IndexIVFPQ> index_;

  // normalize vector
  bool normalize_;

  // first  train data size
  faiss::idx_t train_data_size_;
};

}  // namespace dingodb

#endif  // DINGODB_VECTOR_INDEX_RAW_IVF_PQ_H_  // NOLINT
