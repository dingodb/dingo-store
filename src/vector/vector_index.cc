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

#include <cstdint>
#include <memory>
#include <string>

#include "bthread/bthread.h"
#include "butil/compiler_specific.h"
#include "butil/status.h"
#include "common/constant.h"
#include "fmt/core.h"
#include "gflags/gflags.h"
#include "proto/common.pb.h"
#include "proto/error.pb.h"
#include "server/server.h"
#include "vector/codec.h"
#include "vector/vector_index_snapshot_manager.h"

namespace dingodb {

DEFINE_uint64(hnsw_need_save_count, 10000, "hnsw need save count");

void VectorIndex::SetSnapshotLogId(int64_t snapshot_log_id) {
  this->snapshot_log_id.store(snapshot_log_id, std::memory_order_relaxed);
}

int64_t VectorIndex::ApplyLogId() const { return apply_log_id.load(std::memory_order_relaxed); }

void VectorIndex::SetApplyLogId(int64_t apply_log_id) {
  this->apply_log_id.store(apply_log_id, std::memory_order_relaxed);
}

int64_t VectorIndex::SnapshotLogId() const { return snapshot_log_id.load(std::memory_order_relaxed); }

butil::Status VectorIndex::Save(const std::string& /*path*/) {
  // Save need the caller to do LockWrite() and UnlockWrite()
  return butil::Status(pb::error::Errno::EVECTOR_NOT_SUPPORT, "this vector index do not implement save");
}

butil::Status VectorIndex::Load(const std::string& /*path*/) {
  return butil::Status(pb::error::Errno::EVECTOR_NOT_SUPPORT, "this vector index do not implement load");
}

butil::Status VectorIndex::GetCount([[maybe_unused]] int64_t& count) {
  return butil::Status(pb::error::Errno::EVECTOR_NOT_SUPPORT, "this vector index do not implement get count");
}

butil::Status VectorIndex::GetDeletedCount([[maybe_unused]] int64_t& deleted_count) {
  return butil::Status(pb::error::Errno::EVECTOR_NOT_SUPPORT, "this vector index do not implement get deleted count");
}

butil::Status VectorIndex::GetMemorySize([[maybe_unused]] int64_t& memory_size) {
  return butil::Status(pb::error::Errno::EVECTOR_NOT_SUPPORT, "this vector index do not implement get memory size");
}

VectorIndexWrapper::~VectorIndexWrapper() {
  ClearVectorIndex();
  if (snapshot_set_ != nullptr) {
    snapshot_set_->ClearSnapshot();
  }
  bthread_mutex_destroy(&vector_index_mutex_);
}

std::shared_ptr<VectorIndexWrapper> VectorIndexWrapper::New(int64_t id,
                                                            pb::common::VectorIndexParameter index_parameter) {
  auto vector_index_wrapper =
      std::make_shared<VectorIndexWrapper>(id, index_parameter, Constant::kVectorIndexSaveSnapshotThresholdWriteKeyNum);
  if (vector_index_wrapper != nullptr) {
    if (!vector_index_wrapper->Init()) {
      return nullptr;
    }
  }

  return vector_index_wrapper;
}

std::shared_ptr<VectorIndexWrapper> VectorIndexWrapper::GetSelf() { return shared_from_this(); }

bool VectorIndexWrapper::Init() {
  if (!worker_->Init()) {
    DINGO_LOG(ERROR) << fmt::format("[vector_index.wrapper][index_id({})] Init vector index wrapper failed.", Id());
    return false;
  }
  return true;
}

void VectorIndexWrapper::Destroy() {
  DINGO_LOG(INFO) << fmt::format("[vector_index.wrapper][index_id({})] vector index destroy.", Id());
  stop_.store(true);
  worker_->Destroy();
}

bool VectorIndexWrapper::Recover() {
  auto status = LoadMeta();
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("[vector_index.wrapper][index_id({})] vector index recover failed, error: {}", Id(),
                                    status.error_str());
    return false;
  }

  // recover snapshot_set_
  std::vector<std::string> snapshot_paths;
  VectorIndexSnapshotManager::GetSnapshotList(this->Id(), snapshot_paths);

  for (const auto& snapshot_path : snapshot_paths) {
    auto new_snapshot = vector_index::SnapshotMeta::New(this->Id(), snapshot_path);
    if (!new_snapshot->Init()) {
      DINGO_LOG(ERROR) << fmt::format(
          "[vector_index.wrapper][index_id({})] vector index recover init snapshot_meta faild, path: {}", Id(),
          snapshot_path);
      continue;
    }

    DINGO_LOG(INFO) << fmt::format("[vector_index.wrapper][index_id({})] vector index recover snapshot_meta, path: {}",
                                   Id(), snapshot_path);
    snapshot_set_->AddSnapshot(new_snapshot);
  }

  if (IsHoldVectorIndex()) {
    DINGO_LOG(INFO) << fmt::format("[vector_index.wrapper][index_id({})] need bootstrap build vector index.", Id());
    VectorIndexManager::LaunchLoadOrBuildVectorIndex(GetSelf());
  }

  return true;
}

static std::string GenMetaKey(int64_t vector_index_id) {
  return fmt::format("{}_{}", Constant::kVectorIndexApplyLogIdPrefix, vector_index_id);
}

butil::Status VectorIndexWrapper::SaveMeta() {
  auto meta_writer = Server::GetInstance()->GetMetaWriter();
  if (meta_writer == nullptr) {
    return butil::Status(pb::error::EINTERNAL, "meta writer is nullptr.");
  }

  pb::store_internal::VectorIndexMeta meta;
  meta.set_id(id_);
  meta.set_version(version_);
  meta.set_type(static_cast<int>(vector_index_type_));
  meta.set_apply_log_id(ApplyLogId());
  meta.set_snapshot_log_id(SnapshotLogId());
  meta.set_is_hold_vector_index(IsHoldVectorIndex());

  auto kv = std::make_shared<pb::common::KeyValue>();
  kv->set_key(GenMetaKey(id_));
  kv->set_value(meta.SerializeAsString());
  if (!meta_writer->Put(kv)) {
    return butil::Status(pb::error::EINTERNAL, "Write vector index meta failed.");
  }

  return butil::Status();
}

butil::Status VectorIndexWrapper::LoadMeta() {
  auto meta_reader = Server::GetInstance()->GetMetaReader();
  if (meta_reader == nullptr) {
    return butil::Status(pb::error::EINTERNAL, "meta reader is nullptr.");
  }

  auto kv = meta_reader->Get(GenMetaKey(id_));
  if (kv == nullptr) {
    return butil::Status(pb::error::EINTERNAL, "Get vector index meta failed");
  }

  if (kv->value().empty()) {
    return butil::Status();
  }

  pb::store_internal::VectorIndexMeta meta;
  if (meta.ParsePartialFromArray(kv->value().data(), kv->value().size())) {
    version_ = meta.version();
    vector_index_type_ = static_cast<pb::common::VectorIndexType>(meta.type());
    SetApplyLogId(meta.apply_log_id());
    SetSnapshotLogId(meta.snapshot_log_id());
  } else {
    DINGO_LOG(WARNING) << fmt::format("[vector_index.wrapper][index_id({})] prase vector index meta failed.", Id());
  }

  SetIsHoldVectorIndex(meta.is_hold_vector_index());

  return butil::Status();
}

int64_t VectorIndexWrapper::ApplyLogId() { return apply_log_id_.load(); }

void VectorIndexWrapper::SetApplyLogId(int64_t apply_log_id) { apply_log_id_.store(apply_log_id); }

void VectorIndexWrapper::SaveApplyLogId(int64_t apply_log_id) {
  SetApplyLogId(apply_log_id);
  SaveMeta();
}

int64_t VectorIndexWrapper::SnapshotLogId() { return snapshot_log_id_.load(); }

void VectorIndexWrapper::SetSnapshotLogId(int64_t snapshot_log_id) { snapshot_log_id_.store(snapshot_log_id); }
void VectorIndexWrapper::SaveSnapshotLogId(int64_t snapshot_log_id) {
  SetSnapshotLogId(snapshot_log_id);
  SaveMeta();
}

bool VectorIndexWrapper::IsSwitchingVectorIndex() { return is_switching_vector_index_.load(); }

void VectorIndexWrapper::SetIsSwitchingVectorIndex(bool is_switching) {
  is_switching_vector_index_.store(is_switching);
}

bool VectorIndexWrapper::IsHoldVectorIndex() const { return is_hold_vector_index_.load(); }

void VectorIndexWrapper::SetIsHoldVectorIndex(bool need) {
  is_hold_vector_index_.store(need);
  SaveMeta();
}

void VectorIndexWrapper::UpdateVectorIndex(VectorIndexPtr vector_index, const std::string& reason) {
  DINGO_LOG(INFO) << fmt::format("[vector_index.wrapper][index_id({})] update vector index, reason({})", Id(), reason);
  // Check vector index is stop
  if (IsStop()) {
    DINGO_LOG(WARNING) << fmt::format("[vector_index.wrapper][index_id({})] vector index is stop.", Id());
    return;
  }

  {
    BAIDU_SCOPED_LOCK(vector_index_mutex_);

    int old_active_index = active_index_.load();
    int new_active_index = old_active_index == 0 ? 1 : 0;
    vector_indexs_[new_active_index] = vector_index;
    active_index_.store(new_active_index);
    share_vector_index_ = nullptr;
    ready_.store(true);
    ++version_;

    int64_t apply_log_id = ApplyLogId();
    int64_t snapshot_log_id = SnapshotLogId();
    DINGO_LOG(INFO) << fmt::format(
        "[vector_index.wrapper][index_id({})] update vector index, apply_log_id({}/{}) snapshot_log_id({}/{}) "
        "reason({})",
        Id(), apply_log_id, vector_index->ApplyLogId(), snapshot_log_id, vector_index->SnapshotLogId(), reason);
    if (apply_log_id < vector_index->ApplyLogId()) {
      SetApplyLogId(vector_index->ApplyLogId());
    }
    if (snapshot_log_id < vector_index->SnapshotLogId()) {
      SetSnapshotLogId(vector_index->SnapshotLogId());
    }

    vector_indexs_[old_active_index] = nullptr;

    SaveMeta();
  }
}

void VectorIndexWrapper::ClearVectorIndex() {
  DINGO_LOG(INFO) << fmt::format("[vector_index.wrapper][index_id({})] Clear all vector index", Id());

  BAIDU_SCOPED_LOCK(vector_index_mutex_);

  ready_.store(false);
  vector_indexs_[0] = nullptr;
  vector_indexs_[1] = nullptr;
  share_vector_index_ = nullptr;
}

VectorIndexPtr VectorIndexWrapper::GetOwnVectorIndex() {
  BAIDU_SCOPED_LOCK(vector_index_mutex_);
  return vector_indexs_[active_index_.load()];
}

VectorIndexPtr VectorIndexWrapper::GetVectorIndex() {
  BAIDU_SCOPED_LOCK(vector_index_mutex_);

  if (share_vector_index_ != nullptr) {
    return share_vector_index_;
  }

  return vector_indexs_[active_index_.load()];
}

VectorIndexPtr VectorIndexWrapper::ShareVectorIndex() {
  BAIDU_SCOPED_LOCK(vector_index_mutex_);
  return share_vector_index_;
}

void VectorIndexWrapper::SetShareVectorIndex(VectorIndexPtr vector_index) {
  BAIDU_SCOPED_LOCK(vector_index_mutex_);

  share_vector_index_ = vector_index;

  // During split, there may occur leader change, set ready_ to true can improve the availablidy of vector index
  // Because follower is also do force rebuild too, so in this scenario follower is equivalent to leader
  ready_.store(true);
}

bool VectorIndexWrapper::ExecuteTask(TaskRunnablePtr task) {
  if (worker_ == nullptr) {
    return false;
  }

  bool ret = worker_->Execute(task);
  if (ret) {
    IncPendingTaskNum();
  }

  return ret;
}

int VectorIndexWrapper::PendingTaskNum() { return pending_task_num_.load(); }

void VectorIndexWrapper::IncPendingTaskNum() { pending_task_num_.fetch_add(1); }

void VectorIndexWrapper::DecPendingTaskNum() { pending_task_num_.fetch_sub(1); }

int32_t VectorIndexWrapper::GetDimension() {
  auto vector_index = GetVectorIndex();
  if (vector_index == nullptr) {
    return 0;
  }
  return vector_index->GetDimension();
}

butil::Status VectorIndexWrapper::GetCount(int64_t& count) {
  auto vector_index = GetOwnVectorIndex();
  if (vector_index == nullptr) {
    return butil::Status(pb::error::EVECTOR_INDEX_NOT_FOUND, "vector index %lu is not ready.", Id());
  }

  return vector_index->GetCount(count);
}

butil::Status VectorIndexWrapper::GetDeletedCount(int64_t& deleted_count) {
  auto vector_index = GetOwnVectorIndex();
  if (vector_index == nullptr) {
    return butil::Status(pb::error::EVECTOR_INDEX_NOT_FOUND, "vector index %lu is not ready.", Id());
  }

  return vector_index->GetDeletedCount(deleted_count);
}

butil::Status VectorIndexWrapper::GetMemorySize(int64_t& memory_size) {
  auto vector_index = GetOwnVectorIndex();
  if (vector_index == nullptr) {
    return butil::Status(pb::error::EVECTOR_INDEX_NOT_FOUND, "vector index %lu is not ready.", Id());
  }

  return vector_index->GetMemorySize(memory_size);
}

bool VectorIndexWrapper::IsExceedsMaxElements() {
  auto vector_index = GetVectorIndex();
  if (vector_index == nullptr) {
    return true;
  }
  return vector_index->IsExceedsMaxElements();
}

bool VectorIndexWrapper::NeedToRebuild() {
  auto vector_index = GetOwnVectorIndex();
  if (vector_index == nullptr) {
    return false;
  }

  return vector_index->NeedToRebuild();
}

bool VectorIndexWrapper::SupportSave() {
  auto vector_index = GetOwnVectorIndex();
  if (vector_index == nullptr) {
    return false;
  }

  return vector_index->SupportSave();
}

bool VectorIndexWrapper::NeedToSave(int64_t last_save_log_behind) {
  if (Type() == pb::common::VECTOR_INDEX_TYPE_FLAT) {
    return false;
  }
  auto vector_index = GetOwnVectorIndex();
  if (vector_index == nullptr) {
    return false;
  }
  int64_t element_count = 0, deleted_count = 0;
  auto status = vector_index->GetCount(element_count);
  if (!status.ok()) {
    return false;
  }
  status = vector_index->GetDeletedCount(deleted_count);
  if (!status.ok()) {
    return false;
  }

  if (element_count == 0 && deleted_count == 0) {
    DINGO_LOG(INFO) << fmt::format(
        "[vector_index.wrapper][index_id({})] vector index need_to_save=false: element count is 0 and deleted count is "
        "0, element_count={} "
        "deleted_count={}",
        Id(), element_count, deleted_count);
    return false;
  }

  if (SnapshotLogId() == 0) {
    DINGO_LOG(INFO) << fmt::format(
        "[vector_index.wrapper][index_id({})] vector index need_to_save=true: snapshot_log_id is 0", Id());
    last_save_write_key_count_ = write_key_count_;
    return true;
  }

  if (last_save_log_behind > FLAGS_hnsw_need_save_count) {
    DINGO_LOG(INFO) << fmt::format(
        "[vector_index.wrapper][index_id({})] vector index need_to_save=true: last_save_log_behind={} "
        "FLAGS_hnsw_need_save_count={}",
        Id(), last_save_log_behind, FLAGS_hnsw_need_save_count);
    last_save_write_key_count_ = write_key_count_;
    return true;
  }

  if ((write_key_count_ - last_save_write_key_count_) >= save_snapshot_threshold_write_key_num_) {
    DINGO_LOG(INFO) << fmt::format(
        "[vector_index.wrapper][index_id({})] vector index need_to_save=true: write_key_count {}/{}/{}", Id(),
        write_key_count_, last_save_write_key_count_, save_snapshot_threshold_write_key_num_);
    last_save_write_key_count_ = write_key_count_;
    return true;
  }

  DINGO_LOG(INFO) << fmt::format(
      "[vector_index.wrapper][index_id({})] vector index need_to_save=false: last_save_log_behind={} "
      "hnsw_need_save_count={} write_key_count={}/{}/{}",
      Id(), last_save_log_behind, FLAGS_hnsw_need_save_count, write_key_count_, last_save_write_key_count_,
      save_snapshot_threshold_write_key_num_);

  return false;
}

butil::Status VectorIndexWrapper::Add(const std::vector<pb::common::VectorWithId>& vector_with_ids) {
  if (!IsReady()) {
    DINGO_LOG(WARNING) << fmt::format("[vector_index.wrapper][index_id({})] vector index is not ready.", Id());
    return butil::Status(pb::error::EVECTOR_INDEX_NOT_FOUND, "vector index %lu is not ready.", Id());
  }

  // Switch vector index wait
  int count = 0;
  while (IsSwitchingVectorIndex()) {
    DINGO_LOG(INFO) << fmt::format("[vector_index.wrapper][index_id({})] vector index switch waiting, count({})", Id(),
                                   ++count);
    bthread_usleep(1000 * 100);
  }

  auto vector_index = GetVectorIndex();
  if (vector_index == nullptr) {
    DINGO_LOG(WARNING) << fmt::format("[vector_index.wrapper][index_id({})] vector index is not ready.", Id());
    return butil::Status(pb::error::EVECTOR_INDEX_NOT_FOUND, "vector index %lu is not ready.", Id());
  }

  auto status = vector_index->Add(vector_with_ids);
  if (BAIDU_UNLIKELY(pb::error::Errno::EVECTOR_NOT_TRAIN == status.error_code())) {
    std::vector<float> train_datas;
    train_datas.reserve(vector_index->GetDimension() * vector_with_ids.size());
    for (const auto& vector_with_id : vector_with_ids) {
      train_datas.insert(train_datas.end(), vector_with_id.vector().float_values().begin(),
                         vector_with_id.vector().float_values().end());
    }
    status = vector_index->Train(train_datas);
    if (BAIDU_LIKELY(status.ok())) {
      // try again
      status = vector_index->Add(vector_with_ids);
    } else {
      DINGO_LOG(ERROR) << fmt::format("[vector_index.wrapper][index_id({})] train failed size : {}", Id(),
                                      train_datas.size());
      return status;
    }
  }
  if (status.ok()) {
    write_key_count_ += vector_with_ids.size();
  }
  return status;
}

butil::Status VectorIndexWrapper::Upsert(const std::vector<pb::common::VectorWithId>& vector_with_ids) {
  if (!IsReady()) {
    DINGO_LOG(WARNING) << fmt::format("[vector_index.wrapper][index_id({})] vector index is not ready.", Id());
    return butil::Status(pb::error::EVECTOR_INDEX_NOT_FOUND, "vector index %lu is not ready.", Id());
  }

  // Switch vector index wait
  int count = 0;
  while (IsSwitchingVectorIndex()) {
    DINGO_LOG(INFO) << fmt::format("[vector_index.wrapper][index_id({})] vector index switch waiting, count({})", Id(),
                                   ++count);
    bthread_usleep(1000 * 100);
  }

  auto vector_index = GetVectorIndex();
  if (vector_index == nullptr) {
    DINGO_LOG(WARNING) << fmt::format("[vector_index.wrapper][index_id({})] vector index is not ready.", Id());
    return butil::Status(pb::error::EVECTOR_INDEX_NOT_FOUND, "vector index %lu is not ready.", Id());
  }

  auto status = vector_index->Upsert(vector_with_ids);
  if (BAIDU_UNLIKELY(pb::error::Errno::EVECTOR_NOT_TRAIN == status.error_code())) {
    std::vector<float> train_datas;
    train_datas.reserve(vector_index->GetDimension() * vector_with_ids.size());
    for (const auto& vector_with_id : vector_with_ids) {
      train_datas.insert(train_datas.end(), vector_with_id.vector().float_values().begin(),
                         vector_with_id.vector().float_values().end());
    }
    status = vector_index->Train(train_datas);
    if (BAIDU_LIKELY(status.ok())) {
      // try again
      status = vector_index->Upsert(vector_with_ids);
    } else {
      DINGO_LOG(ERROR) << fmt::format("[vector_index.wrapper][index_id({})] train failed size : {}", Id(),
                                      train_datas.size());
      return status;
    }
  }
  if (status.ok()) {
    write_key_count_ += vector_with_ids.size();
  }
  return status;
}

butil::Status VectorIndexWrapper::Delete(const std::vector<int64_t>& delete_ids) {
  if (!IsReady()) {
    DINGO_LOG(WARNING) << fmt::format("[vector_index.wrapper][index_id({})] vector index is not ready.", Id());
    return butil::Status(pb::error::EVECTOR_INDEX_NOT_FOUND, "vector index %lu is not ready.", Id());
  }

  // Switch vector index wait
  int count = 0;
  while (IsSwitchingVectorIndex()) {
    DINGO_LOG(INFO) << fmt::format("[vector_index.wrapper][index_id({})] vector index switch waiting, count({})", Id(),
                                   ++count);
    bthread_usleep(1000 * 100);
  }

  auto vector_index = GetVectorIndex();
  if (vector_index == nullptr) {
    DINGO_LOG(WARNING) << fmt::format("[vector_index.wrapper][index_id({})] vector index is not ready.", Id());
    return butil::Status(pb::error::EVECTOR_INDEX_NOT_FOUND, "vector index %lu is not ready.", Id());
  }

  auto status = vector_index->Delete(delete_ids);
  if (status.ok()) {
    write_key_count_ += delete_ids.size();
  }
  return status;
}

butil::Status VectorIndexWrapper::Search(std::vector<pb::common::VectorWithId> vector_with_ids, uint32_t topk,
                                         const pb::common::Range& region_range,
                                         std::vector<std::shared_ptr<VectorIndex::FilterFunctor>> filters,
                                         std::vector<pb::index::VectorWithDistanceResult>& results, bool reconstruct,
                                         const pb::common::VectorSearchParameter& parameter) {
  if (!IsReady()) {
    DINGO_LOG(WARNING) << fmt::format("[vector_index.wrapper][index_id({})] vector index is not ready.", Id());
    return butil::Status(pb::error::EVECTOR_INDEX_NOT_FOUND, "vector index %lu is not ready.", Id());
  }
  auto vector_index = GetVectorIndex();
  if (vector_index == nullptr) {
    DINGO_LOG(WARNING) << fmt::format("[vector_index.wrapper][index_id({})] vector index is not ready.", Id());
    return butil::Status(pb::error::EVECTOR_INDEX_NOT_FOUND, "vector index %lu is not ready.", Id());
  }

  const auto& index_range = vector_index->Range();
  if (region_range.start_key() != index_range.start_key() || region_range.end_key() != index_range.end_key()) {
    int64_t min_vector_id = VectorCodec::DecodeVectorId(region_range.start_key());
    int64_t max_vector_id = VectorCodec::DecodeVectorId(region_range.end_key());
    max_vector_id = max_vector_id > 0 ? max_vector_id : INT64_MAX;
    if (vector_index->VectorIndexType() == pb::common::VECTOR_INDEX_TYPE_HNSW) {
      filters.push_back(std::make_shared<VectorIndex::RangeFilterFunctor>(min_vector_id, max_vector_id));
    } else if (vector_index->VectorIndexType() == pb::common::VECTOR_INDEX_TYPE_FLAT) {
      // filters.push_back(std::make_shared<VectorIndex::FlatRangeFilterFunctor>(min_vector_id, max_vector_id));
      filters.push_back(std::make_shared<VectorIndex::RangeFilterFunctor>(min_vector_id, max_vector_id));
    } else if (vector_index->VectorIndexType() == pb::common::VECTOR_INDEX_TYPE_IVF_FLAT) {
      filters.push_back(std::make_shared<VectorIndex::RangeFilterFunctor>(min_vector_id, max_vector_id));
    }
  }

  return vector_index->Search(vector_with_ids, topk, filters, results, reconstruct, parameter);
}

}  // namespace dingodb
