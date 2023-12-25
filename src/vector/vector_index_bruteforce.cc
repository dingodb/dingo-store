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

#include "vector/vector_index_bruteforce.h"

#include <atomic>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <future>
#include <map>
#include <memory>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "bthread/mutex.h"
#include "butil/status.h"
#include "common/synchronization.h"
#include "proto/common.pb.h"
#include "proto/error.pb.h"
#include "proto/index.pb.h"

namespace dingodb {

DEFINE_int64(bruteforce_need_save_count, 10000, "bruteforce need save count");

VectorIndexBruteforce::VectorIndexBruteforce(int64_t id, const pb::common::VectorIndexParameter& vector_index_parameter,
                                             const pb::common::RegionEpoch& epoch, const pb::common::Range& range)
    : VectorIndex(id, vector_index_parameter, epoch, range) {
  metric_type_ = vector_index_parameter.bruteforce_parameter().metric_type();
  dimension_ = vector_index_parameter.bruteforce_parameter().dimension();
}

VectorIndexBruteforce::~VectorIndexBruteforce() = default;

butil::Status VectorIndexBruteforce::Upsert(const std::vector<pb::common::VectorWithId>& /*vector_with_ids*/) {
  return butil::Status::OK();
}

butil::Status VectorIndexBruteforce::Add(const std::vector<pb::common::VectorWithId>& /*vector_with_ids*/) {
  return butil::Status::OK();
}

butil::Status VectorIndexBruteforce::Delete(const std::vector<int64_t>& /*delete_ids*/) { return butil::Status::OK(); }

butil::Status VectorIndexBruteforce::Search(std::vector<pb::common::VectorWithId> /*vector_with_ids*/,
                                            uint32_t /*topk*/, std::vector<std::shared_ptr<FilterFunctor>> /*filters*/,
                                            bool, const pb::common::VectorSearchParameter&,
                                            std::vector<pb::index::VectorWithDistanceResult>& /*results*/) {
  return butil::Status(pb::error::Errno::EVECTOR_NOT_SUPPORT, "not support");
}

butil::Status VectorIndexBruteforce::RangeSearch(std::vector<pb::common::VectorWithId> /*vector_with_ids*/,
                                                 float /*radius*/,
                                                 std::vector<std::shared_ptr<VectorIndex::FilterFunctor>> /*filters*/,
                                                 bool /*reconstruct*/,
                                                 const pb::common::VectorSearchParameter& /*parameter*/,
                                                 std::vector<pb::index::VectorWithDistanceResult>& /*results*/) {
  return butil::Status(pb::error::Errno::EVECTOR_NOT_SUPPORT, "not support");
}

void VectorIndexBruteforce::LockWrite() {}

void VectorIndexBruteforce::UnlockWrite() {}

bool VectorIndexBruteforce::SupportSave() { return true; }

butil::Status VectorIndexBruteforce::Save(const std::string& path) {
  std::ofstream log_file(path);
  if (!log_file.is_open()) {
    return butil::Status(pb::error::Errno::EINTERNAL, "open file failed");
  }
  log_file << "Hello BruteForce" << '\n';
  log_file.close();
  return butil::Status::OK();
}

butil::Status VectorIndexBruteforce::Load(const std::string& /*path*/) { return butil::Status::OK(); }

int32_t VectorIndexBruteforce::GetDimension() { return this->dimension_; }

pb::common::MetricType VectorIndexBruteforce::GetMetricType() { return this->metric_type_; }

butil::Status VectorIndexBruteforce::GetCount(int64_t& /*count*/) {
  return butil::Status(pb::error::EVECTOR_NOT_SUPPORT, "not support");
}

butil::Status VectorIndexBruteforce::GetDeletedCount(int64_t& deleted_count) {
  deleted_count = 0;
  return butil::Status::OK();
}

butil::Status VectorIndexBruteforce::GetMemorySize(int64_t& memory_size) {
  memory_size = 0;
  return butil::Status::OK();
}

bool VectorIndexBruteforce::IsExceedsMaxElements() { return false; }

bool VectorIndexBruteforce::NeedToSave(int64_t last_save_log_behind) {
  return last_save_log_behind > FLAGS_bruteforce_need_save_count;
}

}  // namespace dingodb
