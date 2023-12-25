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

#ifndef DINGODB_VECTOR_INDEX_IVF_PQ_H_  // NOLINT
#define DINGODB_VECTOR_INDEX_IVF_PQ_H_

#include <array>
#include <atomic>
#include <cassert>
#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "bthread/mutex.h"
#include "butil/status.h"
#include "common/logging.h"
#include "common/synchronization.h"
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
#include "vector/vector_index_flat.h"
#include "vector/vector_index_raw_ivf_pq.h"

namespace dingodb {

class VectorIndexIvfPq : public VectorIndex {
 public:
  explicit VectorIndexIvfPq(int64_t id, const pb::common::VectorIndexParameter& vector_index_parameter,
                            const pb::common::RegionEpoch& epoch, const pb::common::Range& ranges);

  ~VectorIndexIvfPq() override;

  VectorIndexIvfPq(const VectorIndexIvfPq& rhs) = delete;
  VectorIndexIvfPq& operator=(const VectorIndexIvfPq& rhs) = delete;
  VectorIndexIvfPq(VectorIndexIvfPq&& rhs) = delete;
  VectorIndexIvfPq& operator=(VectorIndexIvfPq&& rhs) = delete;

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

  pb::common::VectorIndexType VectorIndexSubType() override;

 private:
  void Init();

  bool DoIsTrained();

  // train failed. reset
  void Reset();

  // Dimension of the elements
  faiss::idx_t dimension_;

  // only support L2 and IP
  pb::common::MetricType metric_type_;

  RWLock rw_lock_;

  size_t nlist_;

  size_t nsubvector_;

  int32_t nbits_per_idx_;

  std::unique_ptr<VectorIndexFlat> index_flat_;

  std::unique_ptr<VectorIndexRawIvfPq> index_raw_ivf_pq_;

  enum class IndexTypeInIvfPq : char {
    kUnknow = 0,
    kFlat = 1,
    kIvfPq = 2,
  };
  IndexTypeInIvfPq index_type_in_ivf_pq_;

  template <typename FLAT_FUNC_PTR, typename PQ_FUNC_PTR, typename... Args>
  butil::Status InvokeConcreteFunction(const char* name, FLAT_FUNC_PTR flat_func_ptr, PQ_FUNC_PTR pq_func_ptr,
                                       bool use_glog, Args&&... args);
};

}  // namespace dingodb

#endif  // DINGODB_VECTOR_INDEX_IVF_PQ_H_  // NOLINT
