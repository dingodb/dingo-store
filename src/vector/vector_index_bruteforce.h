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

#ifndef DINGODB_VECTOR_INDEX_BRUTEFORCE_H_  // NOLINT
#define DINGODB_VECTOR_INDEX_BRUTEFORCE_H_

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
#include "fmt/core.h"
#include "proto/common.pb.h"
#include "proto/error.pb.h"
#include "vector/vector_index.h"

namespace dingodb {

// VectorIndexBruteforce is a simple vector index implementation
// which is used for testing and benchmarking.
// In this vector index, we do not implement any index structure, any vector index add, del, search or range search
// All of them are done by brute force, which is processed by VectorReader.
class VectorIndexBruteforce : public VectorIndex {
 public:
  explicit VectorIndexBruteforce(int64_t id, const pb::common::VectorIndexParameter& vector_index_parameter,
                                 const pb::common::RegionEpoch& epoch, const pb::common::Range& ranges);

  ~VectorIndexBruteforce() override;

  VectorIndexBruteforce(const VectorIndexBruteforce& rhs) = delete;
  VectorIndexBruteforce& operator=(const VectorIndexBruteforce& rhs) = delete;
  VectorIndexBruteforce(VectorIndexBruteforce&& rhs) = delete;
  VectorIndexBruteforce& operator=(VectorIndexBruteforce&& rhs) = delete;

  butil::Status Save(const std::string& path) override;
  butil::Status Load(const std::string& path) override;

  butil::Status Add(const std::vector<pb::common::VectorWithId>& vector_with_ids) override;

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
  butil::Status GetCount([[maybe_unused]] int64_t& count) override;
  butil::Status GetDeletedCount([[maybe_unused]] int64_t& deleted_count) override;
  butil::Status GetMemorySize([[maybe_unused]] int64_t& memory_size) override;
  bool IsExceedsMaxElements() override;
  butil::Status Train([[maybe_unused]] const std::vector<float>& train_datas) override { return butil::Status::OK(); }
  butil::Status Train([[maybe_unused]] const std::vector<pb::common::VectorWithId>& vectors) override {
    return butil::Status::OK();
  }

  bool NeedToRebuild() override { return false; }

  bool NeedToSave(int64_t last_save_log_behind) override;

 private:
  // Dimension of the elements
  faiss::idx_t dimension_;

  // only support L2 and IP
  pb::common::MetricType metric_type_;
};

}  // namespace dingodb

#endif  // DINGODB_VECTOR_INDEX_BRUTEFORCE_H_  // NOLINT
