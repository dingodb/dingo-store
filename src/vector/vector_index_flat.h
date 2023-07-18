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
#include "faiss/Index.h"
#include "faiss/IndexFlat.h"
#include "faiss/IndexIDMap.h"
#include "faiss/MetricType.h"
#include "faiss/impl/IDSelector.h"
#include "fmt/core.h"
#include "proto/common.pb.h"
#include "proto/error.pb.h"
#include "vector/vector_index.h"

namespace dingodb {

class VectorIndexFlat : public VectorIndex {
 public:
  explicit VectorIndexFlat(uint64_t id, const pb::common::VectorIndexParameter& vector_index_parameter);

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

  // not exist add. if exist update
  butil::Status Upsert(const std::vector<pb::common::VectorWithId>& vector_with_ids) override;

  butil::Status Delete(const std::vector<uint64_t>& delete_ids) override;

  butil::Status Search(std::vector<pb::common::VectorWithId> vector_with_ids, uint32_t topk,
                       std::vector<pb::index::VectorWithDistanceResult>& results, bool reconstruct = false) override;

  butil::Status SetOnline() override;
  butil::Status SetOffline() override;
  bool IsOnline() override;

  void LockWrite() override;
  void UnlockWrite() override;

  butil::Status GetCount([[maybe_unused]] uint64_t& count) override;

  butil::Status NeedToRebuild([[maybe_unused]] bool& need_to_rebuild,
                              [[maybe_unused]] uint64_t last_save_log_behind) override;
  butil::Status NeedToSave([[maybe_unused]] bool& need_to_save,
                           [[maybe_unused]] uint64_t last_save_log_behind) override;

 private:
  // Dimension of the elements
  faiss::idx_t dimension_;

  // only support L2 and IP
  pb::common::MetricType metric_type_;

  std::unique_ptr<faiss::Index> raw_index_;

  std::unique_ptr<faiss::IndexIDMap> index_;

  bthread_mutex_t mutex_;
  std::atomic<bool> is_online_;
};

}  // namespace dingodb

#endif  // DINGODB_VECTOR_INDEX_FLAT_H_  // NOLINT
