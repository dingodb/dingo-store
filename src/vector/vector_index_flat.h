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

#ifndef DINGODB_VECTOR_INDEX_FLAT_H_  // NOLINT
#define DINGODB_VECTOR_INDEX_FLAT_H_

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
#include "common/synchronization.h"
#include "faiss/Index.h"
#include "faiss/IndexFlat.h"
#include "faiss/IndexIDMap.h"
#include "faiss/MetricType.h"
#include "faiss/impl/IDSelector.h"
#include "faiss/utils/distances.h"
#include "fmt/core.h"
#include "proto/common.pb.h"
#include "proto/error.pb.h"
#include "vector/vector_index.h"

namespace dingodb {

// Filter vector id
class FlatIDSelector : public faiss::IDSelector {
 public:
  FlatIDSelector(std::vector<std::shared_ptr<VectorIndex::FilterFunctor>> filters) : filters_(filters) {}
  ~FlatIDSelector() override = default;
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

class VectorIndexFlat : public VectorIndex {
 public:
  explicit VectorIndexFlat(int64_t id, const pb::common::VectorIndexParameter& vector_index_parameter,
                           const pb::common::RegionEpoch& epoch, const pb::common::Range& ranges);

  ~VectorIndexFlat() override;

  VectorIndexFlat(const VectorIndexFlat& rhs) = delete;
  VectorIndexFlat& operator=(const VectorIndexFlat& rhs) = delete;
  VectorIndexFlat(VectorIndexFlat&& rhs) = delete;
  VectorIndexFlat& operator=(VectorIndexFlat&& rhs) = delete;

  butil::Status Save(const std::string& path) override;
  butil::Status Load(const std::string& path) override;

  // in FLAT index, add two vector with same id will cause data conflict
  butil::Status Add(const std::vector<pb::common::VectorWithId>& vector_with_ids) override;

  butil::Status AddOrUpsert(const std::vector<pb::common::VectorWithId>& vector_with_ids, bool is_upsert);

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
  bool SupportSave() override;

  int32_t GetDimension() override;
  pb::common::MetricType GetMetricType() override;
  butil::Status GetCount(int64_t& count) override;
  butil::Status GetDeletedCount(int64_t& deleted_count) override;
  butil::Status GetMemorySize(int64_t& memory_size) override;
  bool IsExceedsMaxElements() override;
  butil::Status Train([[maybe_unused]] const std::vector<float>& train_datas) override { return butil::Status::OK(); }
  butil::Status Train([[maybe_unused]] const std::vector<pb::common::VectorWithId>& vectors) override {
    return butil::Status::OK();
  }

  bool NeedToRebuild() override { return false; }

  bool NeedToSave(int64_t last_save_log_behind) override;

 private:
  [[deprecated("faiss fix bug. never use.")]] void SearchWithParam(faiss::idx_t n, const faiss::Index::component_t* x,
                                                                   faiss::idx_t k, faiss::Index::distance_t* distances,
                                                                   faiss::idx_t* labels,
                                                                   std::shared_ptr<FlatIDSelector> filters);

  void DoRangeSearch(faiss::idx_t n, const faiss::Index::component_t* x, faiss::Index::distance_t radius,
                     faiss::RangeSearchResult* result,
                     std::vector<std::shared_ptr<VectorIndex::FilterFunctor>> filters);

  // Dimension of the elements
  faiss::idx_t dimension_;

  // only support L2 and IP
  pb::common::MetricType metric_type_;

  std::unique_ptr<faiss::Index> raw_index_;

  std::unique_ptr<faiss::IndexIDMap2> index_id_map2_;

  RWLock rw_lock_;

  // normalize vector
  bool normalize_;
};

}  // namespace dingodb

#endif  // DINGODB_VECTOR_INDEX_FLAT_H_  // NOLINT
