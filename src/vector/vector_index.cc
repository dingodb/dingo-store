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

#include <cassert>
#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "bthread/bthread.h"
#include "butil/status.h"
#include "common/constant.h"
#include "common/helper.h"
#include "common/logging.h"
#include "config/config_manager.h"
#include "fmt/core.h"
#include "proto/common.pb.h"
#include "proto/error.pb.h"
#include "server/server.h"
#include "vector/codec.h"
#include "vector/vector_index_snapshot_manager.h"

namespace dingodb {

DEFINE_uint32(vector_write_batch_size_per_task, 16, "vector write batch size per task");
DEFINE_uint32(vector_read_batch_size_per_task, 1, "vector read batch size per task");

// split VectorWithId set to multi batch
static void SplitVectorWithId(const std::vector<pb::common::VectorWithId>& vector_with_ids, int batch_size,
                              std::vector<std::vector<pb::common::VectorWithId>>& vector_with_id_batchs) {
  auto mut_vector_with_ids = const_cast<std::vector<pb::common::VectorWithId>&>(vector_with_ids);
  for (int i = 0; i < vector_with_ids.size(); i += batch_size) {
    std::vector<pb::common::VectorWithId> vector_with_id_batch;
    for (int j = i; j < i + batch_size && j < vector_with_ids.size(); ++j) {
      vector_with_id_batch.push_back(std::move(mut_vector_with_ids[j]));
    }

    vector_with_id_batchs.push_back(std::move(vector_with_id_batch));
  }
}

template <typename Function>
butil::Status ParallelRun(ThreadPoolPtr thread_pool,
                          const std::vector<std::vector<pb::common::VectorWithId>>& vector_with_id_batchs,
                          bool is_priority, Function fn) {
  std::vector<butil::Status> statuses(vector_with_id_batchs.size());

  uint64_t start_time = Helper::TimestampMs();
  std::vector<ThreadPool::TaskPtr> tasks;
  for (uint32_t i = 0; i < vector_with_id_batchs.size(); ++i) {
    auto task = thread_pool->ExecuteTask([&, i](void*) { statuses[i] = fn(vector_with_id_batchs[i], i); }, nullptr,
                                         is_priority ? 1 : 0);

    if (task != nullptr) {
      tasks.push_back(task);
    }
  }

  for (auto& task : tasks) {
    task->Join();
  }

  DINGO_LOG(INFO) << fmt::format("batch_size: {} inner_batch_size: {} elapsed_time: {}", vector_with_id_batchs.size(),
                                 vector_with_id_batchs[0].size(), Helper::TimestampMs() - start_time);

  for (auto& status : statuses) {
    if (!status.ok()) {
      return status;
    }
  }

  return butil::Status();
}

VectorIndex::VectorIndex(int64_t id, const pb::common::VectorIndexParameter& vector_index_parameter,
                         const pb::common::RegionEpoch& epoch, const pb::common::Range& range,
                         ThreadPoolPtr thread_pool)
    : id(id),
      apply_log_id(0),
      snapshot_log_id(0),
      vector_index_parameter(vector_index_parameter),
      epoch(epoch),
      range(range),
      thread_pool(thread_pool) {
  vector_index_type = vector_index_parameter.vector_index_type();
  DINGO_LOG(DEBUG) << fmt::format("[new.VectorIndex][id({})]", id);
}

VectorIndex::~VectorIndex() { DINGO_LOG(DEBUG) << fmt::format("[delete.VectorIndex][id({})]", id); }

void VectorIndex::SetSnapshotLogId(int64_t snapshot_log_id) {
  this->snapshot_log_id.store(snapshot_log_id, std::memory_order_relaxed);
}

int64_t VectorIndex::ApplyLogId() const { return apply_log_id.load(std::memory_order_relaxed); }

void VectorIndex::SetApplyLogId(int64_t apply_log_id) {
  this->apply_log_id.store(apply_log_id, std::memory_order_relaxed);
}

int64_t VectorIndex::SnapshotLogId() const { return snapshot_log_id.load(std::memory_order_relaxed); }

pb::common::RegionEpoch VectorIndex::Epoch() const { return epoch; };

pb::common::Range VectorIndex::Range() const { return range; }

void VectorIndex::SetEpochAndRange(const pb::common::RegionEpoch& epoch, const pb::common::Range& range) {
  DINGO_LOG(INFO) << fmt::format("[vector_index.raw][id({})] set epoch({}->{}) and range({}->{})", id,
                                 Helper::RegionEpochToString(this->epoch), Helper::RegionEpochToString(epoch),
                                 Helper::RangeToString(this->range), Helper::RangeToString(range));
  this->epoch = epoch;
  this->range = range;
}

butil::Status VectorIndex::Add(const std::vector<pb::common::VectorWithId>& vector_with_ids, bool) {
  return Add(vector_with_ids);
}

butil::Status VectorIndex::AddByParallel(const std::vector<pb::common::VectorWithId>& vector_with_ids,
                                         bool is_priority) {
  if (VectorIndexType() == pb::common::VECTOR_INDEX_TYPE_HNSW) {
    // parallel in inner
    return Add(vector_with_ids, is_priority);
  } else {
    // parallel in here
    std::vector<std::vector<pb::common::VectorWithId>> vector_with_id_batchs;
    SplitVectorWithId(vector_with_ids, FLAGS_vector_write_batch_size_per_task, vector_with_id_batchs);

    return ParallelRun(thread_pool, vector_with_id_batchs, is_priority,
                       [&](const std::vector<pb::common::VectorWithId>& vector_with_ids, uint32_t) -> butil::Status {
                         return Add(vector_with_ids);
                       });
  }
}

butil::Status VectorIndex::Upsert(const std::vector<pb::common::VectorWithId>& vector_with_ids, bool) {
  return Upsert(vector_with_ids);
}

butil::Status VectorIndex::UpsertByParallel(const std::vector<pb::common::VectorWithId>& vector_with_ids,
                                            bool is_priority) {
  if (VectorIndexType() == pb::common::VECTOR_INDEX_TYPE_HNSW) {
    // parallel in inner
    return Upsert(vector_with_ids, is_priority);
  } else {
    // parallel in here
    std::vector<std::vector<pb::common::VectorWithId>> vector_with_id_batchs;
    SplitVectorWithId(vector_with_ids, FLAGS_vector_write_batch_size_per_task, vector_with_id_batchs);
    return ParallelRun(thread_pool, vector_with_id_batchs, is_priority,
                       [&](const std::vector<pb::common::VectorWithId>& vector_with_ids, uint32_t) -> butil::Status {
                         return Upsert(vector_with_ids);
                       });
  }
}

butil::Status VectorIndex::Delete(const std::vector<int64_t>& delete_ids, bool) { return Delete(delete_ids); }

butil::Status VectorIndex::SearchByParallel(const std::vector<pb::common::VectorWithId>& vector_with_ids, uint32_t topk,
                                            const std::vector<std::shared_ptr<FilterFunctor>>& filters,
                                            bool reconstruct, const pb::common::VectorSearchParameter& parameter,
                                            std::vector<pb::index::VectorWithDistanceResult>& results) {
  if (VectorIndexType() == pb::common::VECTOR_INDEX_TYPE_HNSW) {
    // parallel in inner
    return Search(vector_with_ids, topk, filters, reconstruct, parameter, results);
  } else {
    // parallel in here
    results.resize(vector_with_ids.size());

    std::vector<std::vector<pb::common::VectorWithId>> vector_with_id_batchs;
    SplitVectorWithId(vector_with_ids, FLAGS_vector_read_batch_size_per_task, vector_with_id_batchs);

    return ParallelRun(
        thread_pool, vector_with_id_batchs, true,
        [&](const std::vector<pb::common::VectorWithId>& vector_with_ids, uint32_t index) -> butil::Status {
          std::vector<pb::index::VectorWithDistanceResult> part_results;
          auto status = Search(vector_with_ids, topk, filters, reconstruct, parameter, part_results);

          for (int i = 0; i < part_results.size(); ++i) {
            results[index + i].Swap(&part_results[i]);
          }

          return status;
        });
  }
}

butil::Status VectorIndex::RangeSearchByParallel(
    const std::vector<pb::common::VectorWithId>& vector_with_ids, float radius,
    const std::vector<std::shared_ptr<VectorIndex::FilterFunctor>>& filters, bool reconstruct,
    const pb::common::VectorSearchParameter& parameter, std::vector<pb::index::VectorWithDistanceResult>& results) {
  if (VectorIndexType() == pb::common::VECTOR_INDEX_TYPE_HNSW) {
    // parallel in inner
    return RangeSearch(vector_with_ids, radius, filters, reconstruct, parameter, results);
  } else {
    // parallel in here
    results.resize(vector_with_ids.size());

    std::vector<std::vector<pb::common::VectorWithId>> vector_with_id_batchs;
    SplitVectorWithId(vector_with_ids, FLAGS_vector_read_batch_size_per_task, vector_with_id_batchs);

    return ParallelRun(
        thread_pool, vector_with_id_batchs, true,
        [&](const std::vector<pb::common::VectorWithId>& vector_with_ids, uint32_t index) -> butil::Status {
          std::vector<pb::index::VectorWithDistanceResult> part_results;
          auto status = RangeSearch(vector_with_ids, radius, filters, reconstruct, parameter, part_results);

          for (int i = 0; i < part_results.size(); ++i) {
            results[index + i].Swap(&part_results[i]);
          }

          return status;
        });
  }
}

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

VectorIndexWrapper::VectorIndexWrapper(int64_t id, pb::common::VectorIndexParameter index_parameter,
                                       int64_t save_snapshot_threshold_write_key_num)
    : id_(id),
      vector_index_type_(index_parameter.vector_index_type()),
      ready_(false),
      stop_(false),
      is_switching_vector_index_(false),
      apply_log_id_(0),
      snapshot_log_id_(0),
      index_parameter_(index_parameter),
      is_hold_vector_index_(false),
      pending_task_num_(0),
      loadorbuilding_num_(0),
      rebuilding_num_(0),
      saving_num_(0),
      save_snapshot_threshold_write_key_num_(save_snapshot_threshold_write_key_num) {
  snapshot_set_ = vector_index::SnapshotMetaSet::New(id);
  bthread_mutex_init(&vector_index_mutex_, nullptr);
  DINGO_LOG(DEBUG) << fmt::format("[new.VectorIndexWrapper][id({})]", id_);
}

VectorIndexWrapper::~VectorIndexWrapper() {
  ClearVectorIndex("destruct");
  if (snapshot_set_ != nullptr) {
    snapshot_set_->ClearSnapshot();
  }

  bthread_mutex_destroy(&vector_index_mutex_);
  DINGO_LOG(DEBUG) << fmt::format("[delete.VectorIndexWrapper][id({})]", id_);
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

bool VectorIndexWrapper::Init() { return true; }  // NOLINT

void VectorIndexWrapper::Destroy() {
  DINGO_LOG(INFO) << fmt::format("[vector_index.wrapper][index_id({})] vector index destroy.", Id());
  stop_.store(true);
}

bool VectorIndexWrapper::Recover() {
  auto status = LoadMeta();
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("[vector_index.wrapper][index_id({})] vector index recover failed, error: {}", Id(),
                                    status.error_str());
    return false;
  }

  // recover snapshot_set_
  auto snapshot_paths = VectorIndexSnapshotManager::GetSnapshotList(this->Id());

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

  if (IsTempHoldVectorIndex()) {
    DINGO_LOG(INFO) << fmt::format("[vector_index.wrapper][index_id({})] need bootstrap build vector index.", Id());
    // use slow load
    VectorIndexManager::LaunchLoadAsyncBuildVectorIndex(GetSelf(), false, false, 0, "recover");
  }

  return true;
}

static std::string GenMetaKey(int64_t vector_index_id) {
  return fmt::format("{}_{}", Constant::kVectorIndexApplyLogIdPrefix, vector_index_id);
}

butil::Status VectorIndexWrapper::SaveMeta() {
  auto meta_writer = Server::GetInstance().GetMetaWriter();
  if (meta_writer == nullptr) {
    return butil::Status(pb::error::EINTERNAL, "meta writer is nullptr.");
  }

  pb::store_internal::VectorIndexMeta meta;
  meta.set_id(id_);
  meta.set_version(version_);
  meta.set_type(static_cast<int>(vector_index_type_));
  meta.set_apply_log_id(ApplyLogId());
  meta.set_snapshot_log_id(SnapshotLogId());
  meta.set_is_hold_vector_index(IsTempHoldVectorIndex());

  auto kv = std::make_shared<pb::common::KeyValue>();
  kv->set_key(GenMetaKey(id_));
  kv->set_value(meta.SerializeAsString());
  if (!meta_writer->Put(kv)) {
    return butil::Status(pb::error::EINTERNAL, "Write vector index meta failed.");
  }

  return butil::Status();
}

butil::Status VectorIndexWrapper::LoadMeta() {
  auto meta_reader = Server::GetInstance().GetMetaReader();
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

  SetIsTempHoldVectorIndex(meta.is_hold_vector_index());

  return butil::Status();
}

int64_t VectorIndexWrapper::LastBuildEpochVersion() {
  if (snapshot_set_ == nullptr) {
    return 0;
  }

  auto snapshot = snapshot_set_->GetLastSnapshot();
  if (snapshot == nullptr) {
    return 0;
  }

  return snapshot->Epoch().version();
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

bool VectorIndexWrapper::IsTempHoldVectorIndex() const { return is_hold_vector_index_.load(); }

void VectorIndexWrapper::SetIsTempHoldVectorIndex(bool need) {
  DINGO_LOG(INFO) << fmt::format("[vector_index.wrapper][index_id({})] set vector index hold({}->{})", Id(),
                                 IsTempHoldVectorIndex(), need);
  is_hold_vector_index_.store(need);
  SaveMeta();
}

void VectorIndexWrapper::UpdateVectorIndex(VectorIndexPtr vector_index, const std::string& trace) {
  DINGO_LOG(INFO) << fmt::format(
      "[vector_index.wrapper][index_id({})][trace({})] update vector index, epoch({}) range({})", Id(), trace,
      Helper::RegionEpochToString(vector_index->Epoch()), VectorCodec::DecodeRangeToString(vector_index->Range()));
  // Check vector index is stop
  if (IsStop()) {
    DINGO_LOG(WARNING) << fmt::format("[vector_index.wrapper][index_id({})] vector index is stop.", Id());
    return;
  }
  if (!IsPermanentHoldVectorIndex(this->Id()) && !IsTempHoldVectorIndex()) {
    DINGO_LOG(WARNING) << fmt::format(
        "[vector_index.wrapper][index_id({})][trace({})] vector index is not hold. is_perm_hold: {}, is_temp_hold: {}",
        Id(), trace, IsPermanentHoldVectorIndex(this->Id()), IsTempHoldVectorIndex());
    return;
  }

  {
    BAIDU_SCOPED_LOCK(vector_index_mutex_);

    auto old_vector_index = vector_index_;
    vector_index_ = vector_index;

    share_vector_index_ = nullptr;

    if (sibling_vector_index_ != nullptr &&
        Helper::IsContainRange(vector_index->Range(), sibling_vector_index_->Range())) {
      sibling_vector_index_ = nullptr;
    }

    ++version_;

    ready_.store(true);

    int64_t apply_log_id = ApplyLogId();
    int64_t snapshot_log_id = SnapshotLogId();
    DINGO_LOG(INFO) << fmt::format(
        "[vector_index.wrapper][index_id({})][trace({})] update vector index, apply_log_id({}/{}) "
        "snapshot_log_id({}/{}).",
        Id(), trace, apply_log_id, vector_index->ApplyLogId(), snapshot_log_id, vector_index->SnapshotLogId());
    if (apply_log_id < vector_index->ApplyLogId()) {
      SetApplyLogId(vector_index->ApplyLogId());
    }
    if (snapshot_log_id < vector_index->SnapshotLogId()) {
      SetSnapshotLogId(vector_index->SnapshotLogId());
    }

    SaveMeta();
  }
}

void VectorIndexWrapper::ClearVectorIndex(const std::string& trace) {
  DINGO_LOG(INFO) << fmt::format("[vector_index.wrapper][index_id({})][trace({})] Clear all vector index", Id(), trace);

  BAIDU_SCOPED_LOCK(vector_index_mutex_);

  ready_.store(false);
  vector_index_ = nullptr;
  share_vector_index_ = nullptr;
  sibling_vector_index_ = nullptr;
}

VectorIndexPtr VectorIndexWrapper::GetOwnVectorIndex() {
  BAIDU_SCOPED_LOCK(vector_index_mutex_);
  return vector_index_;
}

VectorIndexPtr VectorIndexWrapper::GetVectorIndex() {
  BAIDU_SCOPED_LOCK(vector_index_mutex_);

  if (share_vector_index_ != nullptr) {
    return share_vector_index_;
  }

  return vector_index_;
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

VectorIndexPtr VectorIndexWrapper::SiblingVectorIndex() {
  BAIDU_SCOPED_LOCK(vector_index_mutex_);
  return sibling_vector_index_;
}

void VectorIndexWrapper::SetSiblingVectorIndex(VectorIndexPtr vector_index) {
  BAIDU_SCOPED_LOCK(vector_index_mutex_);
  sibling_vector_index_ = vector_index;
}

int32_t VectorIndexWrapper::PendingTaskNum() { return pending_task_num_.load(std::memory_order_relaxed); }
void VectorIndexWrapper::IncPendingTaskNum() { pending_task_num_.fetch_add(1, std::memory_order_relaxed); }
void VectorIndexWrapper::DecPendingTaskNum() { pending_task_num_.fetch_sub(1, std::memory_order_relaxed); }

int32_t VectorIndexWrapper::LoadorbuildingNum() { return loadorbuilding_num_.load(std::memory_order_relaxed); }
void VectorIndexWrapper::IncLoadoruildingNum() { loadorbuilding_num_.fetch_add(1, std::memory_order_relaxed); }
void VectorIndexWrapper::DecLoadoruildingNum() { loadorbuilding_num_.fetch_sub(1, std::memory_order_relaxed); }

int32_t VectorIndexWrapper::RebuildingNum() { return rebuilding_num_.load(std::memory_order_relaxed); }
void VectorIndexWrapper::IncRebuildingNum() { rebuilding_num_.fetch_add(1, std::memory_order_relaxed); }
void VectorIndexWrapper::DecRebuildingNum() { rebuilding_num_.fetch_sub(1, std::memory_order_relaxed); }

int32_t VectorIndexWrapper::SavingNum() { return saving_num_.load(std::memory_order_relaxed); }
void VectorIndexWrapper::IncSavingNum() { saving_num_.fetch_add(1, std::memory_order_relaxed); }
void VectorIndexWrapper::DecSavingNum() { saving_num_.fetch_sub(1, std::memory_order_relaxed); }

int32_t VectorIndexWrapper::GetDimension() {
  auto vector_index = GetVectorIndex();
  if (vector_index == nullptr) {
    return 0;
  }
  return vector_index->GetDimension();
}

pb::common::MetricType VectorIndexWrapper::GetMetricType() {
  auto vector_index = GetVectorIndex();
  if (vector_index == nullptr) {
    return pb::common::MetricType::METRIC_TYPE_L2;
  }
  return vector_index->GetMetricType();
}

butil::Status VectorIndexWrapper::GetCount(int64_t& count) {
  auto vector_index = GetOwnVectorIndex();
  if (vector_index == nullptr) {
    return butil::Status(pb::error::EVECTOR_INDEX_NOT_FOUND, "vector index %lu is not ready.", Id());
  }

  int64_t own_count = 0;
  auto status = vector_index->GetCount(own_count);
  if (!status.ok()) {
    return status;
  }

  int64_t sibling_count = 0;
  auto sibling_vector_index = SiblingVectorIndex();
  if (sibling_vector_index != nullptr) {
    status = sibling_vector_index->GetCount(sibling_count);
    if (!status.ok()) {
      return status;
    }
  }

  count = own_count + sibling_count;
  return status;
}

butil::Status VectorIndexWrapper::GetDeletedCount(int64_t& deleted_count) {
  auto vector_index = GetOwnVectorIndex();
  if (vector_index == nullptr) {
    return butil::Status(pb::error::EVECTOR_INDEX_NOT_FOUND, "vector index %lu is not ready.", Id());
  }

  int64_t own_deleted_count = 0;
  auto status = vector_index->GetDeletedCount(own_deleted_count);
  if (!status.ok()) {
    return status;
  }

  int64_t sibling_deleted_count = 0;
  auto sibling_vector_index = SiblingVectorIndex();
  if (sibling_vector_index != nullptr) {
    status = sibling_vector_index->GetDeletedCount(sibling_deleted_count);
    if (!status.ok()) {
      return status;
    }
  }

  deleted_count = own_deleted_count + sibling_deleted_count;

  return status;
}

butil::Status VectorIndexWrapper::GetMemorySize(int64_t& memory_size) {
  auto vector_index = GetOwnVectorIndex();
  if (vector_index == nullptr) {
    return butil::Status(pb::error::EVECTOR_INDEX_NOT_FOUND, "vector index %lu is not ready.", Id());
  }

  int64_t own_memory_size = 0;
  auto status = vector_index->GetMemorySize(own_memory_size);
  if (!status.ok()) {
    return status;
  }

  int64_t sibling_memory_size = 0;
  auto sibling_vector_index = SiblingVectorIndex();
  if (sibling_vector_index != nullptr) {
    status = sibling_vector_index->GetMemorySize(sibling_memory_size);
    if (!status.ok()) {
      return status;
    }
  }

  memory_size = own_memory_size + sibling_memory_size;

  return status;
}

bool VectorIndexWrapper::IsExceedsMaxElements() {
  auto vector_index = GetVectorIndex();
  if (vector_index == nullptr) {
    return true;
  }

  auto sibling_vector_index = SiblingVectorIndex();
  if (sibling_vector_index != nullptr) {
    if (!sibling_vector_index->IsExceedsMaxElements()) {
      return false;
    }
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

bool VectorIndexWrapper::NeedToSave(std::string& reason) {
  auto vector_index = GetOwnVectorIndex();
  if (vector_index == nullptr) {
    return false;
  }

  if (Helper::InvalidRange(vector_index->Range())) {
    reason = "range invalid";
    last_save_write_key_count_ = write_key_count_;
    return true;
  }

  if (SnapshotLogId() == 0) {
    reason = "no snapshot";
    last_save_write_key_count_ = write_key_count_;
    return true;
  }

  int64_t last_save_log_behind = ApplyLogId() - SnapshotLogId();
  bool ret = vector_index->NeedToSave(last_save_log_behind);
  if (ret) {
    reason = fmt::format("raft log gap({}) exceed threshold", last_save_log_behind);
    last_save_write_key_count_ = write_key_count_;

    return ret;
  }

  if ((write_key_count_ - last_save_write_key_count_) >= save_snapshot_threshold_write_key_num_) {
    reason = fmt::format("write key gap({}) exceed threshold({})", write_key_count_ - last_save_write_key_count_,
                         save_snapshot_threshold_write_key_num_);
    last_save_write_key_count_ = write_key_count_;
    return true;
  }

  DINGO_LOG(INFO) << fmt::format(
      "[vector_index.wrapper][index_id({})] not need save, last_save_log_behind={} write_key_count={}/{}/{}", Id(),
      last_save_log_behind, write_key_count_, last_save_write_key_count_, save_snapshot_threshold_write_key_num_);

  return false;
}

// Filter vector id by range
static std::vector<int64_t> FilterVectorId(const std::vector<pb::common::VectorWithId>& vector_with_ids,
                                           const pb::common::Range& range) {
  int64_t begin_vector_id = 0, end_vector_id = 0;
  VectorCodec::DecodeRangeToVectorId(range, begin_vector_id, end_vector_id);

  std::vector<int64_t> result;
  for (const auto& vector_with_id : vector_with_ids) {
    if (vector_with_id.id() >= begin_vector_id && vector_with_id.id() < end_vector_id) {
      result.push_back(vector_with_id.id());
    }
  }

  return result;
}

// Filter vector id by range
static std::vector<int64_t> FilterVectorId(const std::vector<int64_t>& vector_ids, const pb::common::Range& range) {
  int64_t begin_vector_id = 0, end_vector_id = 0;
  VectorCodec::DecodeRangeToVectorId(range, begin_vector_id, end_vector_id);

  std::vector<int64_t> result;
  for (const auto vector_id : vector_ids) {
    if (vector_id >= begin_vector_id && vector_id < end_vector_id) {
      result.push_back(vector_id);
    }
  }

  return result;
}

// Filter VectorWithId by range
static std::vector<pb::common::VectorWithId> FilterVectorWithId(
    const std::vector<pb::common::VectorWithId>& vector_with_ids, const pb::common::Range& range) {
  auto mut_vector_with_ids = const_cast<std::vector<pb::common::VectorWithId>&>(vector_with_ids);

  int64_t begin_vector_id = 0, end_vector_id = 0;
  VectorCodec::DecodeRangeToVectorId(range, begin_vector_id, end_vector_id);

  std::vector<pb::common::VectorWithId> result;
  for (auto& vector_with_id : mut_vector_with_ids) {
    if (vector_with_id.id() >= begin_vector_id && vector_with_id.id() < end_vector_id) {
      pb::common::VectorWithId temp_vector_with_id;
      result.push_back(temp_vector_with_id);
      auto& ref_vector_with_id = result.at(result.size() - 1);

      ref_vector_with_id.set_id(vector_with_id.id());
      ref_vector_with_id.mutable_vector()->Swap(vector_with_id.mutable_vector());
    }
  }

  return result;
}

butil::Status VectorIndexWrapper::Add(const std::vector<pb::common::VectorWithId>& vector_with_ids) {
  if (!IsReady()) {
    DINGO_LOG(WARNING) << fmt::format("[vector_index.wrapper][index_id({})] vector index is not ready.", Id());
    return butil::Status(pb::error::EVECTOR_INDEX_NOT_FOUND, "vector index %lu is not ready.", Id());
  }

  // Waiting switch vector index
  int count = 0;
  while (IsSwitchingVectorIndex()) {
    DINGO_LOG(INFO) << fmt::format("[vector_index.wrapper][index_id({})] waiting vector index switch, count({})", Id(),
                                   ++count);
    bthread_usleep(1000 * 100);
  }

  auto vector_index = GetVectorIndex();
  if (vector_index == nullptr) {
    DINGO_LOG(WARNING) << fmt::format("[vector_index.wrapper][index_id({})] vector index is not ready.", Id());
    return butil::Status(pb::error::EVECTOR_INDEX_NOT_FOUND, "vector index %lu is not ready.", Id());
  }

  // Exist sibling vector index, so need to separate add vector.
  auto sibling_vector_index = SiblingVectorIndex();
  if (sibling_vector_index != nullptr) {
    auto status =
        sibling_vector_index->AddByParallel(FilterVectorWithId(vector_with_ids, sibling_vector_index->Range()));
    if (!status.ok()) {
      return status;
    }

    status = vector_index->AddByParallel(FilterVectorWithId(vector_with_ids, vector_index->Range()));
    if (!status.ok()) {
      sibling_vector_index->Delete(FilterVectorId(vector_with_ids, sibling_vector_index->Range()));
      return status;
    }

    write_key_count_ += vector_with_ids.size();

    return status;
  }

  auto status = vector_index->AddByParallel(vector_with_ids);
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
    DINGO_LOG(INFO) << fmt::format("[vector_index.wrapper][index_id({})] waiting vector index switch, count({})", Id(),
                                   ++count);
    bthread_usleep(1000 * 100);
  }

  auto vector_index = GetVectorIndex();
  if (vector_index == nullptr) {
    DINGO_LOG(WARNING) << fmt::format("[vector_index.wrapper][index_id({})] vector index is not ready.", Id());
    return butil::Status(pb::error::EVECTOR_INDEX_NOT_FOUND, "vector index %lu is not ready.", Id());
  }

  // Exist sibling vector index, so need to separate upsert vector.
  auto sibling_vector_index = SiblingVectorIndex();
  if (sibling_vector_index != nullptr) {
    auto status =
        sibling_vector_index->UpsertByParallel(FilterVectorWithId(vector_with_ids, sibling_vector_index->Range()));
    if (!status.ok()) {
      return status;
    }

    status = vector_index->UpsertByParallel(FilterVectorWithId(vector_with_ids, vector_index->Range()));
    if (!status.ok()) {
      sibling_vector_index->Delete(FilterVectorId(vector_with_ids, sibling_vector_index->Range()));
      return status;
    }

    write_key_count_ += vector_with_ids.size();

    return status;
  }

  auto status = vector_index->UpsertByParallel(vector_with_ids);
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

  // Exist sibling vector index, so need to separate delete vector.
  auto sibling_vector_index = SiblingVectorIndex();
  if (sibling_vector_index != nullptr) {
    auto status = sibling_vector_index->Delete(FilterVectorId(delete_ids, sibling_vector_index->Range()));
    if (!status.ok()) {
      return status;
    }

    status = vector_index->Delete(FilterVectorId(delete_ids, vector_index->Range()));
    if (status.ok()) {
      write_key_count_ += delete_ids.size();
    }
    return status;
  }

  auto status = vector_index->Delete(delete_ids);
  if (status.ok()) {
    write_key_count_ += delete_ids.size();
  }
  return status;
}

static void MergeSearchResult(uint32_t topk, pb::index::VectorWithDistanceResult& input_1,
                              pb::index::VectorWithDistanceResult& input_2,
                              pb::index::VectorWithDistanceResult& results) {
  if (topk == 0) return;
  int input_1_size = input_1.vector_with_distances_size();
  int input_2_size = input_2.vector_with_distances_size();
  auto* vector_with_distances_1 = input_1.mutable_vector_with_distances();
  auto* vector_with_distances_2 = input_2.mutable_vector_with_distances();

  int i = 0, j = 0;
  while (i < input_1_size && j < input_2_size) {
    auto& distance_1 = vector_with_distances_1->at(i);
    auto& distance_2 = vector_with_distances_2->at(j);
    if (distance_1.distance() <= distance_2.distance()) {
      ++i;
      results.add_vector_with_distances()->Swap(&distance_1);
    } else {
      ++j;
      results.add_vector_with_distances()->Swap(&distance_2);
    }

    if (results.vector_with_distances_size() >= topk) {
      return;
    }
  }

  for (; i < input_1_size; ++i) {
    auto& distance = vector_with_distances_1->at(i);
    results.add_vector_with_distances()->Swap(&distance);
    if (results.vector_with_distances_size() >= topk) {
      return;
    }
  }

  for (; j < input_2_size; ++j) {
    auto& distance = vector_with_distances_2->at(j);
    results.add_vector_with_distances()->Swap(&distance);
    if (results.vector_with_distances_size() >= topk) {
      return;
    }
  }
}

static void MergeSearchResults(uint32_t topk, std::vector<pb::index::VectorWithDistanceResult>& input_1,
                               std::vector<pb::index::VectorWithDistanceResult>& input_2,
                               std::vector<pb::index::VectorWithDistanceResult>& results) {
  assert(input_1.size() == input_2.size());

  results.resize(input_1.size());
  for (int i = 0; i < input_1.size(); ++i) {
    MergeSearchResult(topk, input_1[i], input_2[i], results[i]);
  }
}

butil::Status VectorIndexWrapper::Search(std::vector<pb::common::VectorWithId> vector_with_ids, uint32_t topk,
                                         const pb::common::Range& region_range,
                                         std::vector<std::shared_ptr<VectorIndex::FilterFunctor>>& filters,
                                         bool reconstruct, const pb::common::VectorSearchParameter& parameter,
                                         std::vector<pb::index::VectorWithDistanceResult>& results) {
  if (!IsReady()) {
    DINGO_LOG(WARNING) << fmt::format("[vector_index.wrapper][index_id({})] vector index is not ready.", Id());
    return butil::Status(pb::error::EVECTOR_INDEX_NOT_FOUND, "vector index %lu is not ready.", Id());
  }
  auto vector_index = GetVectorIndex();
  if (vector_index == nullptr) {
    DINGO_LOG(WARNING) << fmt::format("[vector_index.wrapper][index_id({})] vector index is not ready.", Id());
    return butil::Status(pb::error::EVECTOR_INDEX_NOT_FOUND, "vector index %lu is not ready.", Id());
  }

  // Exist sibling vector index, so need to separate search vector.
  auto sibling_vector_index = SiblingVectorIndex();
  if (sibling_vector_index != nullptr) {
    std::vector<pb::index::VectorWithDistanceResult> results_1;
    auto status =
        sibling_vector_index->SearchByParallel(vector_with_ids, topk, filters, reconstruct, parameter, results_1);
    if (!status.ok()) {
      return status;
    }

    std::vector<pb::index::VectorWithDistanceResult> results_2;
    status = vector_index->SearchByParallel(vector_with_ids, topk, filters, reconstruct, parameter, results_2);
    if (!status.ok()) {
      return status;
    }

    MergeSearchResults(topk, results_1, results_2, results);
    return status;
  }

  const auto& index_range = vector_index->Range();
  if (region_range.start_key() != index_range.start_key() || region_range.end_key() != index_range.end_key()) {
    int64_t min_vector_id = 0, max_vector_id = 0;
    VectorCodec::DecodeRangeToVectorId(region_range, min_vector_id, max_vector_id);
    auto ret = VectorIndexWrapper::SetVectorIndexRangeFilter(vector_index, filters, min_vector_id, max_vector_id);
    if (!ret.ok()) {
      DINGO_LOG(ERROR) << fmt::format("[vector_index.wrapper][index_id({})] set vector index filter failed, error: {}",
                                      Id(), ret.error_str());
      return ret;
    }
  }

  return vector_index->SearchByParallel(vector_with_ids, topk, filters, reconstruct, parameter, results);
}

static void MergeRangeSearchResults(std::vector<pb::index::VectorWithDistanceResult>& input_1,
                                    std::vector<pb::index::VectorWithDistanceResult>& input_2,
                                    std::vector<pb::index::VectorWithDistanceResult>& results) {
  assert(input_1.size() == input_2.size());

  results.resize(input_1.size());
  for (int i = 0; i < input_1.size(); ++i) {
    MergeSearchResult(UINT32_MAX, input_1[i], input_2[i], results[i]);
  }
}

butil::Status VectorIndexWrapper::RangeSearch(std::vector<pb::common::VectorWithId> vector_with_ids, float radius,
                                              const pb::common::Range& region_range,
                                              std::vector<std::shared_ptr<VectorIndex::FilterFunctor>> filters,
                                              bool reconstruct, const pb::common::VectorSearchParameter& parameter,
                                              std::vector<pb::index::VectorWithDistanceResult>& results) {
  if (!IsReady()) {
    DINGO_LOG(WARNING) << fmt::format("[vector_index.wrapper][index_id({})] vector index is not ready.", Id());
    return butil::Status(pb::error::EVECTOR_INDEX_NOT_FOUND, "vector index %lu is not ready.", Id());
  }
  auto vector_index = GetVectorIndex();
  if (vector_index == nullptr) {
    DINGO_LOG(WARNING) << fmt::format("[vector_index.wrapper][index_id({})] vector index is not ready.", Id());
    return butil::Status(pb::error::EVECTOR_INDEX_NOT_FOUND, "vector index %lu is not ready.", Id());
  }

  // Exist sibling vector index, so need to separate search vector.
  auto sibling_vector_index = SiblingVectorIndex();
  if (sibling_vector_index != nullptr) {
    std::vector<pb::index::VectorWithDistanceResult> results_1;
    auto status = sibling_vector_index->RangeSearchByParallel(vector_with_ids, radius, filters, reconstruct, parameter,
                                                              results_1);
    if (!status.ok()) {
      return status;
    }

    std::vector<pb::index::VectorWithDistanceResult> results_2;
    status = vector_index->RangeSearchByParallel(vector_with_ids, radius, filters, reconstruct, parameter, results_2);
    if (!status.ok()) {
      return status;
    }

    MergeRangeSearchResults(results_1, results_2, results);
    return status;
  }

  const auto& index_range = vector_index->Range();
  if (region_range.start_key() != index_range.start_key() || region_range.end_key() != index_range.end_key()) {
    int64_t min_vector_id = 0, max_vector_id = 0;
    VectorCodec::DecodeRangeToVectorId(region_range, min_vector_id, max_vector_id);
    auto ret = VectorIndexWrapper::SetVectorIndexRangeFilter(vector_index, filters, min_vector_id, max_vector_id);
    if (!ret.ok()) {
      DINGO_LOG(ERROR) << fmt::format("[vector_index.wrapper][index_id({})] set vector index filter failed, error: {}",
                                      Id(), ret.error_str());
      return ret;
    }
  }

  return vector_index->RangeSearchByParallel(vector_with_ids, radius, filters, reconstruct, parameter, results);
}

bool VectorIndexWrapper::IsPermanentHoldVectorIndex(int64_t region_id) {
  auto config = ConfigManager::GetInstance().GetRoleConfig();
  if (config == nullptr) {
    return true;
  }

  if (!config->GetBool("vector.enable_follower_hold_index")) {
    // If follower, delete vector index.
    if (!Server::GetInstance().IsLeader(region_id)) {
      return false;
    }
  }
  return true;
}

butil::Status VectorIndexWrapper::SetVectorIndexRangeFilter(
    VectorIndexPtr /*vector_index*/, std::vector<std::shared_ptr<VectorIndex::FilterFunctor>>& filters,
    int64_t min_vector_id, int64_t max_vector_id) {
  filters.push_back(std::make_shared<VectorIndex::RangeFilterFunctor>(min_vector_id, max_vector_id));
  return butil::Status::OK();
}

}  // namespace dingodb
