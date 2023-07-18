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

#include "vector/vector_index.h"

#include "butil/status.h"
#include "proto/common.pb.h"
#include "proto/error.pb.h"

namespace dingodb {

VectorIndex::~VectorIndex() = default;

pb::common::VectorIndexType VectorIndex::VectorIndexType() const { return vector_index_type; }

void VectorIndex::SetSnapshotLogIndex(uint64_t snapshot_log_index) {
  this->snapshot_log_index.store(snapshot_log_index, std::memory_order_relaxed);
}

uint64_t VectorIndex::ApplyLogIndex() const { return apply_log_index.load(std::memory_order_relaxed); }

void VectorIndex::SetApplyLogIndex(uint64_t apply_log_index) {
  this->apply_log_index.store(apply_log_index, std::memory_order_relaxed);
}

uint64_t VectorIndex::SnapshotLogIndex() const { return snapshot_log_index.load(std::memory_order_relaxed); }

butil::Status VectorIndex::Save(const std::string& /*path*/) {
  // Save need the caller to do LockWrite() and UnlockWrite()
  return butil::Status(pb::error::Errno::EVECTOR_NOT_SUPPORT, "this vector index do not implement save");
}

butil::Status VectorIndex::Load(const std::string& /*path*/) {
  return butil::Status(pb::error::Errno::EVECTOR_NOT_SUPPORT, "this vector index do not implement load");
}

butil::Status VectorIndex::GetCount([[maybe_unused]] uint64_t& count) {
  return butil::Status(pb::error::Errno::EVECTOR_NOT_SUPPORT, "this vector index do not implement get count");
}

butil::Status VectorIndex::NeedToRebuild([[maybe_unused]] bool& need_to_rebuild,
                                         [[maybe_unused]] uint64_t last_save_log_behind) {
  return butil::Status(pb::error::Errno::EVECTOR_NOT_SUPPORT, "this vector index do not implement need to rebuild");
}

butil::Status VectorIndex::NeedToSave([[maybe_unused]] bool& need_to_save,
                                      [[maybe_unused]] uint64_t last_save_log_behind) {
  return butil::Status(pb::error::Errno::EVECTOR_NOT_SUPPORT, "this vector index do not implement need to rebuild");
}

}  // namespace dingodb
