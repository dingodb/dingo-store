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

#ifndef DINGODB_VECTOR_INDEX_IVF_FLAT_H_  // NOLINT
#define DINGODB_VECTOR_INDEX_IVF_FLAT_H_

#include <array>
#include <atomic>
#include <cassert>
#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "bthread/mutex.h"
#include "butil/status.h"
#include "common/logging.h"
#include "faiss/Index.h"
#include "faiss/IndexFlat.h"
#include "faiss/IndexIDMap.h"
#include "faiss/IndexIVFFlat.h"
#include "faiss/MetricType.h"
#include "faiss/impl/IDSelector.h"
#include "faiss/utils/distances.h"
#include "fmt/core.h"
#include "proto/common.pb.h"
#include "proto/error.pb.h"
#include "vector/vector_index.h"

namespace dingodb {

// Filter vector id
class IvfFlatIDSelector : public faiss::IDSelector {
 public:
  explicit IvfFlatIDSelector(std::vector<std::shared_ptr<VectorIndex::FilterFunctor>> filters) : filters_(filters) {}
  ~IvfFlatIDSelector() override = default;
  bool is_member(faiss::idx_t id) const override {  // NOLINT
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

class VectorIndexIvfFlat : public VectorIndex {
 public:
  explicit VectorIndexIvfFlat(int64_t id, const pb::common::VectorIndexParameter& vector_index_parameter,
                              const pb::common::RegionEpoch& epoch, const pb::common::Range& ranges);

  ~VectorIndexIvfFlat() override;

  VectorIndexIvfFlat(const VectorIndexIvfFlat& rhs) = delete;
  VectorIndexIvfFlat& operator=(const VectorIndexIvfFlat& rhs) = delete;
  VectorIndexIvfFlat(VectorIndexIvfFlat&& rhs) = delete;
  VectorIndexIvfFlat& operator=(VectorIndexIvfFlat&& rhs) = delete;

  butil::Status Save(const std::string& path) override;
  butil::Status Load(const std::string& path) override;
  bool SupportSave() override;

  // in FLAT index, add two vector with same id will cause data conflict
  butil::Status Add(const std::vector<pb::common::VectorWithId>& vector_with_ids) override;

  butil::Status AddOrUpsertWrapper(const std::vector<pb::common::VectorWithId>& vector_with_ids, bool is_upsert);

  // not exist add. if exist update
  butil::Status Upsert(const std::vector<pb::common::VectorWithId>& vector_with_ids) override;

  butil::Status Delete(const std::vector<int64_t>& delete_ids) override;

  butil::Status Search(std::vector<pb::common::VectorWithId> vector_with_ids, uint32_t topk,
                       std::vector<std::shared_ptr<FilterFunctor>> filters, bool reconstruct,
                       const pb::common::VectorSearchParameter& parameter,
                       std::vector<pb::index::VectorWithDistanceResult>& results) override;

  butil::Status RangeSearch(std::vector<pb::common::VectorWithId> vector_with_ids, float radius,
                            std::vector<std::shared_ptr<VectorIndex::FilterFunctor>> filters, bool reconstruct,
                            const pb::common::VectorSearchParameter& parameter,
                            std::vector<pb::index::VectorWithDistanceResult>& results) override;

  void LockWrite() override;
  void UnlockWrite() override;

  int32_t GetDimension() override;
  pb::common::MetricType GetMetricType() override;
  butil::Status GetCount([[maybe_unused]] int64_t& count) override;
  butil::Status GetDeletedCount([[maybe_unused]] int64_t& deleted_count) override;
  butil::Status GetMemorySize([[maybe_unused]] int64_t& memory_size) override;
  bool IsExceedsMaxElements() override;

  butil::Status Train(const std::vector<float>& train_datas) override;
  butil::Status Train([[maybe_unused]] const std::vector<pb::common::VectorWithId>& vectors) override;
  bool NeedToRebuild() override;
  bool NeedTrain() override { return true; }
  bool IsTrained() override;
  bool NeedToSave(int64_t last_save_log_behind) override;

 private:
  void Init();

  bool DoIsTrained();

  // train failed. reset
  void Reset();

  butil::Status AddOrUpsert(const std::vector<pb::common::VectorWithId>& vector_with_ids, bool is_upsert);

  // Dimension of the elements
  faiss::idx_t dimension_;

  // only support L2 and IP
  pb::common::MetricType metric_type_;

  // bthread_mutex_t mutex_;
  RWLock rw_lock_;

  // maybe 1 or vector_index_parameter.ivf_flat_parameter().ncentroids()
  size_t nlist_;

  // from  vector_index_parameter.ivf_flat_parameter().ncentroids()
  size_t nlist_org_;

  std::unique_ptr<faiss::Index> quantizer_;

  std::unique_ptr<faiss::IndexIVFFlat> index_;

  // normalize vector
  bool normalize_;

  // first  train data size
  faiss::idx_t train_data_size_;
};

}  // namespace dingodb

#endif  // DINGODB_VECTOR_INDEX_IVF_FLAT_H_  // NOLINT
