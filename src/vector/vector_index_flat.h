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

  // in FLAT index, add two vector with same id will cause data conlict
  butil::Status Add(uint64_t id, const std::vector<float>& vector) override;

  // not exist add. if exist update
  butil::Status Upsert(uint64_t id, const std::vector<float>& vector) override;

  void Delete(uint64_t id) override;

  butil::Status Save([[maybe_unused]] const std::string& path) override;

  butil::Status Load([[maybe_unused]] const std::string& path) override;

  butil::Status Search(const std::vector<float>& vector, uint32_t topk,
                       std::vector<pb::common::VectorWithDistance>& results) override;

  butil::Status Search(pb::common::VectorWithId vector_with_id, uint32_t topk,
                       std::vector<pb::common::VectorWithDistance>& results) override;

 private:
  // Dimension of the elements
  faiss::idx_t dimension_;

  // only support L2 and IP
  pb::common::MetricType metric_type_;

  std::unique_ptr<faiss::Index> raw_index_;

  std::unique_ptr<faiss::IndexIDMap> index_;

  bthread_mutex_t mutex_;
};

}  // namespace dingodb

#endif  // DINGODB_VECTOR_INDEX_FLAT_H_  // NOLINT
