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

#include "document/document_index.h"

#include <cassert>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "bthread/bthread.h"
#include "butil/status.h"
#include "common/constant.h"
#include "common/helper.h"
#include "common/logging.h"
#include "config/config_manager.h"
#include "document/codec.h"
#include "fmt/core.h"
#include "proto/common.pb.h"
#include "proto/error.pb.h"
#include "server/server.h"

namespace dingodb {

DEFINE_uint32(ivf_vector_write_batch_size_per_task, 256, "ivf vector write batch size per task");
DEFINE_uint32(vector_read_batch_size_per_task, 1, "vector read batch size per task");

DEFINE_uint32(parallel_log_threshold_time_ms, 5000, "parallel log elapsed time");

DocumentIndex::DocumentIndex(int64_t id, const pb::common::DocumentIndexParameter& document_index_parameter,
                             const pb::common::RegionEpoch& epoch, const pb::common::Range& range,
                             ThreadPoolPtr thread_pool)
    : id(id),
      apply_log_id(0),
      snapshot_log_id(0),
      document_index_parameter(document_index_parameter),
      epoch(epoch),
      range(range),
      thread_pool(thread_pool) {
  DINGO_LOG(DEBUG) << fmt::format("[new.DocumentIndex][id({})]", id);
}

DocumentIndex::~DocumentIndex() { DINGO_LOG(DEBUG) << fmt::format("[delete.DocumentIndex][id({})]", id); }

void DocumentIndex::SetSnapshotLogId(int64_t snapshot_log_id) {
  this->snapshot_log_id.store(snapshot_log_id, std::memory_order_relaxed);
}

int64_t DocumentIndex::ApplyLogId() const { return apply_log_id.load(std::memory_order_relaxed); }

void DocumentIndex::SetApplyLogId(int64_t apply_log_id) {
  this->apply_log_id.store(apply_log_id, std::memory_order_relaxed);
}

int64_t DocumentIndex::SnapshotLogId() const { return snapshot_log_id.load(std::memory_order_relaxed); }

pb::common::RegionEpoch DocumentIndex::Epoch() const { return epoch; };

pb::common::Range DocumentIndex::Range() const { return range; }

void DocumentIndex::SetEpochAndRange(const pb::common::RegionEpoch& epoch, const pb::common::Range& range) {
  DINGO_LOG(INFO) << fmt::format("[vector_index.raw][id({})] set epoch({}->{}) and range({}->{})", id,
                                 Helper::RegionEpochToString(this->epoch), Helper::RegionEpochToString(epoch),
                                 Helper::RangeToString(this->range), Helper::RangeToString(range));
  this->epoch = epoch;
  this->range = range;
}

butil::Status DocumentIndex::Add(const std::vector<pb::common::DocumentWithId>& document_with_ids, bool) {
  return Add(document_with_ids);
}

butil::Status DocumentIndex::AddByParallel(const std::vector<pb::common::DocumentWithId>& document_with_ids,
                                           bool is_priority) {
  return Add(document_with_ids, is_priority);
}

butil::Status DocumentIndex::Upsert(const std::vector<pb::common::DocumentWithId>& document_with_ids, bool) {
  return Upsert(document_with_ids);
}

butil::Status DocumentIndex::UpsertByParallel(const std::vector<pb::common::DocumentWithId>& document_with_ids,
                                              bool is_priority) {
  return Upsert(document_with_ids, is_priority);
}

butil::Status DocumentIndex::Delete(const std::vector<int64_t>& delete_ids, bool) { return Delete(delete_ids); }

butil::Status DocumentIndex::DeleteByParallel(const std::vector<int64_t>& delete_ids, bool is_priority) {
  return Delete(delete_ids, is_priority);
}

butil::Status DocumentIndex::SearchByParallel(const std::vector<pb::common::DocumentWithId>& document_with_ids,
                                              uint32_t topk, const std::vector<std::shared_ptr<FilterFunctor>>& filters,
                                              bool reconstruct, const pb::common::DocumentSearchParameter& parameter,
                                              std::vector<pb::document::DocumentWithScoreResult>& results) {
  return Search(document_with_ids, topk, filters, reconstruct, parameter, results);
}

butil::Status DocumentIndex::TrainByParallel(std::vector<float>& /*train_datas*/) { return butil::Status::OK(); }

butil::Status DocumentIndex::Save(const std::string& /*path*/) {
  // Save need the caller to do LockWrite() and UnlockWrite()
  return butil::Status(pb::error::Errno::EVECTOR_NOT_SUPPORT, "this document index do not implement save");
}

butil::Status DocumentIndex::Load(const std::string& /*path*/) {
  return butil::Status(pb::error::Errno::EVECTOR_NOT_SUPPORT, "this document index do not implement load");
}

butil::Status DocumentIndex::GetCount([[maybe_unused]] int64_t& count) {
  return butil::Status(pb::error::Errno::EVECTOR_NOT_SUPPORT, "this document index do not implement get count");
}

butil::Status DocumentIndex::GetDeletedCount([[maybe_unused]] int64_t& deleted_count) {
  return butil::Status(pb::error::Errno::EVECTOR_NOT_SUPPORT, "this document index do not implement get deleted count");
}

butil::Status DocumentIndex::GetMemorySize([[maybe_unused]] int64_t& memory_size) {
  return butil::Status(pb::error::Errno::EVECTOR_NOT_SUPPORT, "this document index do not implement get memory size");
}

DocumentIndexWrapper::DocumentIndexWrapper(int64_t id, pb::common::DocumentIndexParameter index_parameter,
                                           int64_t save_snapshot_threshold_write_key_num)
    : id_(id),
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
  bthread_mutex_init(&vector_index_mutex_, nullptr);
  DINGO_LOG(DEBUG) << fmt::format("[new.DocumentIndexWrapper][id({})]", id_);
}

DocumentIndexWrapper::~DocumentIndexWrapper() {
  ClearDocumentIndex("destruct");

  bthread_mutex_destroy(&vector_index_mutex_);
  DINGO_LOG(DEBUG) << fmt::format("[delete.DocumentIndexWrapper][id({})]", id_);
}

std::shared_ptr<DocumentIndexWrapper> DocumentIndexWrapper::New(int64_t id,
                                                                pb::common::DocumentIndexParameter index_parameter) {
  auto vector_index_wrapper = std::make_shared<DocumentIndexWrapper>(id, index_parameter, UINT64_MAX);
  if (vector_index_wrapper != nullptr) {
    if (!vector_index_wrapper->Init()) {
      return nullptr;
    }
  }

  return vector_index_wrapper;
}

std::shared_ptr<DocumentIndexWrapper> DocumentIndexWrapper::GetSelf() { return shared_from_this(); }

bool DocumentIndexWrapper::Init() { return true; }  // NOLINT

void DocumentIndexWrapper::Destroy() {
  DINGO_LOG(INFO) << fmt::format("[vector_index.wrapper][index_id({})] vector index destroy.", Id());
  stop_.store(true);
}

bool DocumentIndexWrapper::Recover() {
  auto status = LoadMeta();
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("[vector_index.wrapper][index_id({})] vector index recover failed, error: {}", Id(),
                                    status.error_str());
    return false;
  }

  return true;
}

static std::string GenMetaKey(int64_t vector_index_id) {
  return fmt::format("{}_{}", Constant::kDocumentIndexApplyLogIdPrefix, vector_index_id);
}

butil::Status DocumentIndexWrapper::SaveMeta() {
  auto meta_writer = Server::GetInstance().GetMetaWriter();
  if (meta_writer == nullptr) {
    return butil::Status(pb::error::EINTERNAL, "meta writer is nullptr.");
  }

  pb::store_internal::DocumentIndexMeta meta;
  meta.set_id(id_);
  meta.set_version(version_);
  meta.set_apply_log_id(ApplyLogId());
  meta.set_snapshot_log_id(SnapshotLogId());
  meta.set_is_hold_vector_index(IsTempHoldDocumentIndex());

  auto kv = std::make_shared<pb::common::KeyValue>();
  kv->set_key(GenMetaKey(id_));
  kv->set_value(meta.SerializeAsString());
  if (!meta_writer->Put(kv)) {
    return butil::Status(pb::error::EINTERNAL, "Write vector index meta failed.");
  }

  return butil::Status();
}

butil::Status DocumentIndexWrapper::LoadMeta() {
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

  pb::store_internal::DocumentIndexMeta meta;
  if (meta.ParsePartialFromArray(kv->value().data(), kv->value().size())) {
    version_ = meta.version();
    SetApplyLogId(meta.apply_log_id());
    SetSnapshotLogId(meta.snapshot_log_id());
  } else {
    DINGO_LOG(WARNING) << fmt::format("[vector_index.wrapper][index_id({})] prase vector index meta failed.", Id());
  }

  SetIsTempHoldDocumentIndex(meta.is_hold_vector_index());

  return butil::Status();
}

int64_t DocumentIndexWrapper::LastBuildEpochVersion() { return 0; }  // NOLINT

int64_t DocumentIndexWrapper::ApplyLogId() { return apply_log_id_.load(); }

void DocumentIndexWrapper::SetApplyLogId(int64_t apply_log_id) { apply_log_id_.store(apply_log_id); }

void DocumentIndexWrapper::SaveApplyLogId(int64_t apply_log_id) {
  SetApplyLogId(apply_log_id);
  SaveMeta();
}

int64_t DocumentIndexWrapper::SnapshotLogId() { return snapshot_log_id_.load(); }

void DocumentIndexWrapper::SetSnapshotLogId(int64_t snapshot_log_id) { snapshot_log_id_.store(snapshot_log_id); }
void DocumentIndexWrapper::SaveSnapshotLogId(int64_t snapshot_log_id) {
  SetSnapshotLogId(snapshot_log_id);
  SaveMeta();
}

bool DocumentIndexWrapper::IsSwitchingDocumentIndex() { return is_switching_vector_index_.load(); }

void DocumentIndexWrapper::SetIsSwitchingDocumentIndex(bool is_switching) {
  is_switching_vector_index_.store(is_switching);
}

bool DocumentIndexWrapper::IsTempHoldDocumentIndex() const { return is_hold_vector_index_.load(); }

void DocumentIndexWrapper::SetIsTempHoldDocumentIndex(bool need) {
  DINGO_LOG(INFO) << fmt::format("[vector_index.wrapper][index_id({})] set vector index hold({}->{})", Id(),
                                 IsTempHoldDocumentIndex(), need);
  is_hold_vector_index_.store(need);
  SaveMeta();
}

void DocumentIndexWrapper::UpdateDocumentIndex(DocumentIndexPtr vector_index, const std::string& trace) {
  DINGO_LOG(INFO) << fmt::format(
      "[vector_index.wrapper][index_id({})][trace({})] update vector index, epoch({}) range({})", Id(), trace,
      Helper::RegionEpochToString(vector_index->Epoch()), DocumentCodec::DecodeRangeToString(vector_index->Range()));
  // Check vector index is stop
  if (IsStop()) {
    DINGO_LOG(WARNING) << fmt::format("[vector_index.wrapper][index_id({})] vector index is stop.", Id());
    return;
  }
  if (!IsPermanentHoldDocumentIndex(this->Id()) && !IsTempHoldDocumentIndex()) {
    DINGO_LOG(WARNING) << fmt::format(
        "[vector_index.wrapper][index_id({})][trace({})] vector index is not hold. is_perm_hold: {}, is_temp_hold: {}",
        Id(), trace, IsPermanentHoldDocumentIndex(this->Id()), IsTempHoldDocumentIndex());
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

void DocumentIndexWrapper::ClearDocumentIndex(const std::string& trace) {
  DINGO_LOG(INFO) << fmt::format("[vector_index.wrapper][index_id({})][trace({})] Clear all vector index", Id(), trace);

  BAIDU_SCOPED_LOCK(vector_index_mutex_);

  ready_.store(false);
  vector_index_ = nullptr;
  share_vector_index_ = nullptr;
  sibling_vector_index_ = nullptr;
}

DocumentIndexPtr DocumentIndexWrapper::GetOwnDocumentIndex() {
  BAIDU_SCOPED_LOCK(vector_index_mutex_);
  return vector_index_;
}

DocumentIndexPtr DocumentIndexWrapper::GetDocumentIndex() {
  BAIDU_SCOPED_LOCK(vector_index_mutex_);

  if (share_vector_index_ != nullptr) {
    return share_vector_index_;
  }

  return vector_index_;
}

DocumentIndexPtr DocumentIndexWrapper::ShareDocumentIndex() {
  BAIDU_SCOPED_LOCK(vector_index_mutex_);
  return share_vector_index_;
}

void DocumentIndexWrapper::SetShareDocumentIndex(DocumentIndexPtr vector_index) {
  BAIDU_SCOPED_LOCK(vector_index_mutex_);

  share_vector_index_ = vector_index;

  // During split, there may occur leader change, set ready_ to true can improve the availablidy of vector index
  // Because follower is also do force rebuild too, so in this scenario follower is equivalent to leader
  ready_.store(true);
}

DocumentIndexPtr DocumentIndexWrapper::SiblingDocumentIndex() {
  BAIDU_SCOPED_LOCK(vector_index_mutex_);
  return sibling_vector_index_;
}

void DocumentIndexWrapper::SetSiblingDocumentIndex(DocumentIndexPtr vector_index) {
  BAIDU_SCOPED_LOCK(vector_index_mutex_);
  sibling_vector_index_ = vector_index;
}

int32_t DocumentIndexWrapper::PendingTaskNum() { return pending_task_num_.load(std::memory_order_relaxed); }
void DocumentIndexWrapper::IncPendingTaskNum() { pending_task_num_.fetch_add(1, std::memory_order_relaxed); }
void DocumentIndexWrapper::DecPendingTaskNum() { pending_task_num_.fetch_sub(1, std::memory_order_relaxed); }

int32_t DocumentIndexWrapper::LoadorbuildingNum() { return loadorbuilding_num_.load(std::memory_order_relaxed); }
void DocumentIndexWrapper::IncLoadoruildingNum() { loadorbuilding_num_.fetch_add(1, std::memory_order_relaxed); }
void DocumentIndexWrapper::DecLoadoruildingNum() { loadorbuilding_num_.fetch_sub(1, std::memory_order_relaxed); }

int32_t DocumentIndexWrapper::RebuildingNum() { return rebuilding_num_.load(std::memory_order_relaxed); }
void DocumentIndexWrapper::IncRebuildingNum() { rebuilding_num_.fetch_add(1, std::memory_order_relaxed); }
void DocumentIndexWrapper::DecRebuildingNum() { rebuilding_num_.fetch_sub(1, std::memory_order_relaxed); }

int32_t DocumentIndexWrapper::SavingNum() { return saving_num_.load(std::memory_order_relaxed); }
void DocumentIndexWrapper::IncSavingNum() { saving_num_.fetch_add(1, std::memory_order_relaxed); }
void DocumentIndexWrapper::DecSavingNum() { saving_num_.fetch_sub(1, std::memory_order_relaxed); }

int32_t DocumentIndexWrapper::GetDimension() {
  auto vector_index = GetDocumentIndex();
  if (vector_index == nullptr) {
    return 0;
  }
  return vector_index->GetDimension();
}

pb::common::MetricType DocumentIndexWrapper::GetMetricType() {
  auto vector_index = GetDocumentIndex();
  if (vector_index == nullptr) {
    return pb::common::MetricType::METRIC_TYPE_L2;
  }
  return vector_index->GetMetricType();
}

butil::Status DocumentIndexWrapper::GetCount(int64_t& count) {
  auto vector_index = GetOwnDocumentIndex();
  if (vector_index == nullptr) {
    return butil::Status(pb::error::EVECTOR_INDEX_NOT_FOUND, "vector index %lu is not ready.", Id());
  }

  int64_t own_count = 0;
  auto status = vector_index->GetCount(own_count);
  if (!status.ok()) {
    return status;
  }

  int64_t sibling_count = 0;
  auto sibling_vector_index = SiblingDocumentIndex();
  if (sibling_vector_index != nullptr) {
    status = sibling_vector_index->GetCount(sibling_count);
    if (!status.ok()) {
      return status;
    }
  }

  count = own_count + sibling_count;
  return status;
}

butil::Status DocumentIndexWrapper::GetDeletedCount(int64_t& deleted_count) {
  auto vector_index = GetOwnDocumentIndex();
  if (vector_index == nullptr) {
    return butil::Status(pb::error::EVECTOR_INDEX_NOT_FOUND, "vector index %lu is not ready.", Id());
  }

  int64_t own_deleted_count = 0;
  auto status = vector_index->GetDeletedCount(own_deleted_count);
  if (!status.ok()) {
    return status;
  }

  int64_t sibling_deleted_count = 0;
  auto sibling_vector_index = SiblingDocumentIndex();
  if (sibling_vector_index != nullptr) {
    status = sibling_vector_index->GetDeletedCount(sibling_deleted_count);
    if (!status.ok()) {
      return status;
    }
  }

  deleted_count = own_deleted_count + sibling_deleted_count;

  return status;
}

butil::Status DocumentIndexWrapper::GetMemorySize(int64_t& memory_size) {
  auto vector_index = GetOwnDocumentIndex();
  if (vector_index == nullptr) {
    return butil::Status(pb::error::EVECTOR_INDEX_NOT_FOUND, "vector index %lu is not ready.", Id());
  }

  int64_t own_memory_size = 0;
  auto status = vector_index->GetMemorySize(own_memory_size);
  if (!status.ok()) {
    return status;
  }

  int64_t sibling_memory_size = 0;
  auto sibling_vector_index = SiblingDocumentIndex();
  if (sibling_vector_index != nullptr) {
    status = sibling_vector_index->GetMemorySize(sibling_memory_size);
    if (!status.ok()) {
      return status;
    }
  }

  memory_size = own_memory_size + sibling_memory_size;

  return status;
}

bool DocumentIndexWrapper::IsExceedsMaxElements() {
  auto vector_index = GetDocumentIndex();
  if (vector_index == nullptr) {
    return true;
  }

  auto sibling_vector_index = SiblingDocumentIndex();
  if (sibling_vector_index != nullptr) {
    if (!sibling_vector_index->IsExceedsMaxElements()) {
      return false;
    }
  }

  return vector_index->IsExceedsMaxElements();
}

bool DocumentIndexWrapper::NeedToRebuild() {
  auto vector_index = GetOwnDocumentIndex();
  if (vector_index == nullptr) {
    return false;
  }

  return vector_index->NeedToRebuild();
}

bool DocumentIndexWrapper::SupportSave() {
  auto vector_index = GetOwnDocumentIndex();
  if (vector_index == nullptr) {
    return false;
  }

  return vector_index->SupportSave();
}

bool DocumentIndexWrapper::NeedToSave(std::string& reason) {
  auto vector_index = GetOwnDocumentIndex();
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
static std::vector<int64_t> FilterDocumentId(const std::vector<pb::common::DocumentWithId>& document_with_ids,
                                             const pb::common::Range& range) {
  int64_t begin_document_id = 0, end_document_id = 0;
  DocumentCodec::DecodeRangeToDocumentId(range, begin_document_id, end_document_id);

  std::vector<int64_t> result;
  for (const auto& document_with_id : document_with_ids) {
    if (document_with_id.id() >= begin_document_id && document_with_id.id() < end_document_id) {
      result.push_back(document_with_id.id());
    }
  }

  return result;
}

// Filter vector id by range
static std::vector<int64_t> FilterDocumentId(const std::vector<int64_t>& document_ids, const pb::common::Range& range) {
  int64_t begin_document_id = 0, end_document_id = 0;
  DocumentCodec::DecodeRangeToDocumentId(range, begin_document_id, end_document_id);

  std::vector<int64_t> result;
  for (const auto document_id : document_ids) {
    if (document_id >= begin_document_id && document_id < end_document_id) {
      result.push_back(document_id);
    }
  }

  return result;
}

// Filter DocumentWithId by range
static std::vector<pb::common::DocumentWithId> FilterDocumentWithId(
    const std::vector<pb::common::DocumentWithId>& document_with_ids, const pb::common::Range& range) {
  auto mut_document_with_ids = const_cast<std::vector<pb::common::DocumentWithId>&>(document_with_ids);

  int64_t begin_document_id = 0, end_document_id = 0;
  DocumentCodec::DecodeRangeToDocumentId(range, begin_document_id, end_document_id);

  std::vector<pb::common::DocumentWithId> result;
  for (auto& document_with_id : mut_document_with_ids) {
    if (document_with_id.id() >= begin_document_id && document_with_id.id() < end_document_id) {
      pb::common::DocumentWithId temp_document_with_id;
      result.push_back(temp_document_with_id);
      auto& ref_document_with_id = result.at(result.size() - 1);

      ref_document_with_id.set_id(document_with_id.id());
      ref_document_with_id.mutable_document()->Swap(document_with_id.mutable_document());
    }
  }

  return result;
}

butil::Status DocumentIndexWrapper::Add(const std::vector<pb::common::DocumentWithId>& document_with_ids) {
  if (!IsReady()) {
    DINGO_LOG(WARNING) << fmt::format("[vector_index.wrapper][index_id({})] vector index is not ready.", Id());
    return butil::Status(pb::error::EVECTOR_INDEX_NOT_FOUND, "vector index %lu is not ready.", Id());
  }

  // Waiting switch vector index
  int count = 0;
  while (IsSwitchingDocumentIndex()) {
    DINGO_LOG(INFO) << fmt::format("[vector_index.wrapper][index_id({})] waiting vector index switch, count({})", Id(),
                                   ++count);
    bthread_usleep(1000 * 100);
  }

  auto vector_index = GetDocumentIndex();
  if (vector_index == nullptr) {
    DINGO_LOG(WARNING) << fmt::format("[vector_index.wrapper][index_id({})] vector index is not ready.", Id());
    return butil::Status(pb::error::EVECTOR_INDEX_NOT_FOUND, "vector index %lu is not ready.", Id());
  }

  // Exist sibling vector index, so need to separate add vector.
  auto sibling_vector_index = SiblingDocumentIndex();
  if (sibling_vector_index != nullptr) {
    auto status =
        sibling_vector_index->AddByParallel(FilterDocumentWithId(document_with_ids, sibling_vector_index->Range()));
    if (!status.ok()) {
      return status;
    }

    status = vector_index->AddByParallel(FilterDocumentWithId(document_with_ids, vector_index->Range()));
    if (!status.ok()) {
      sibling_vector_index->DeleteByParallel(FilterDocumentId(document_with_ids, sibling_vector_index->Range()), true);
      return status;
    }

    write_key_count_ += document_with_ids.size();

    return status;
  }

  auto status = vector_index->AddByParallel(document_with_ids);
  if (status.ok()) {
    write_key_count_ += document_with_ids.size();
  }
  return status;
}

butil::Status DocumentIndexWrapper::Upsert(const std::vector<pb::common::DocumentWithId>& document_with_ids) {
  if (!IsReady()) {
    DINGO_LOG(WARNING) << fmt::format("[vector_index.wrapper][index_id({})] vector index is not ready.", Id());
    return butil::Status(pb::error::EVECTOR_INDEX_NOT_FOUND, "vector index %lu is not ready.", Id());
  }

  // Switch vector index wait
  int count = 0;
  while (IsSwitchingDocumentIndex()) {
    DINGO_LOG(INFO) << fmt::format("[vector_index.wrapper][index_id({})] waiting vector index switch, count({})", Id(),
                                   ++count);
    bthread_usleep(1000 * 100);
  }

  auto vector_index = GetDocumentIndex();
  if (vector_index == nullptr) {
    DINGO_LOG(WARNING) << fmt::format("[vector_index.wrapper][index_id({})] vector index is not ready.", Id());
    return butil::Status(pb::error::EVECTOR_INDEX_NOT_FOUND, "vector index %lu is not ready.", Id());
  }

  // Exist sibling vector index, so need to separate upsert vector.
  auto sibling_vector_index = SiblingDocumentIndex();
  if (sibling_vector_index != nullptr) {
    auto status =
        sibling_vector_index->UpsertByParallel(FilterDocumentWithId(document_with_ids, sibling_vector_index->Range()));
    if (!status.ok()) {
      return status;
    }

    status = vector_index->UpsertByParallel(FilterDocumentWithId(document_with_ids, vector_index->Range()));
    if (!status.ok()) {
      sibling_vector_index->DeleteByParallel(FilterDocumentId(document_with_ids, sibling_vector_index->Range()), true);
      return status;
    }

    write_key_count_ += document_with_ids.size();

    return status;
  }

  auto status = vector_index->UpsertByParallel(document_with_ids);
  if (status.ok()) {
    write_key_count_ += document_with_ids.size();
  }
  return status;
}

butil::Status DocumentIndexWrapper::Delete(const std::vector<int64_t>& delete_ids) {
  if (!IsReady()) {
    DINGO_LOG(WARNING) << fmt::format("[vector_index.wrapper][index_id({})] vector index is not ready.", Id());
    return butil::Status(pb::error::EVECTOR_INDEX_NOT_FOUND, "vector index %lu is not ready.", Id());
  }

  // Switch vector index wait
  int count = 0;
  while (IsSwitchingDocumentIndex()) {
    DINGO_LOG(INFO) << fmt::format("[vector_index.wrapper][index_id({})] vector index switch waiting, count({})", Id(),
                                   ++count);
    bthread_usleep(1000 * 100);
  }

  auto vector_index = GetDocumentIndex();
  if (vector_index == nullptr) {
    DINGO_LOG(WARNING) << fmt::format("[vector_index.wrapper][index_id({})] vector index is not ready.", Id());
    return butil::Status(pb::error::EVECTOR_INDEX_NOT_FOUND, "vector index %lu is not ready.", Id());
  }

  // Exist sibling vector index, so need to separate delete vector.
  auto sibling_vector_index = SiblingDocumentIndex();
  if (sibling_vector_index != nullptr) {
    auto status =
        sibling_vector_index->DeleteByParallel(FilterDocumentId(delete_ids, sibling_vector_index->Range()), true);
    if (!status.ok()) {
      return status;
    }

    status = vector_index->DeleteByParallel(FilterDocumentId(delete_ids, vector_index->Range()), true);
    if (status.ok()) {
      write_key_count_ += delete_ids.size();
    }
    return status;
  }

  auto status = vector_index->DeleteByParallel(delete_ids, true);
  if (status.ok()) {
    write_key_count_ += delete_ids.size();
  }
  return status;
}

static void MergeSearchResult(uint32_t topk, pb::document::DocumentWithScoreResult& input_1,
                              pb::document::DocumentWithScoreResult& input_2,
                              pb::document::DocumentWithScoreResult& results) {
  if (topk == 0) return;
  int input_1_size = input_1.document_with_scores_size();
  int input_2_size = input_2.document_with_scores_size();
  auto* document_with_scores_1 = input_1.mutable_document_with_scores();
  auto* document_with_scores_2 = input_2.mutable_document_with_scores();

  int i = 0, j = 0;
  while (i < input_1_size && j < input_2_size) {
    auto& distance_1 = document_with_scores_1->at(i);
    auto& distance_2 = document_with_scores_2->at(j);
    if (distance_1.score() <= distance_2.score()) {
      ++i;
      results.add_document_with_scores()->Swap(&distance_1);
    } else {
      ++j;
      results.add_document_with_scores()->Swap(&distance_2);
    }

    if (results.document_with_scores_size() >= topk) {
      return;
    }
  }

  for (; i < input_1_size; ++i) {
    auto& distance = document_with_scores_1->at(i);
    results.add_document_with_scores()->Swap(&distance);
    if (results.document_with_scores_size() >= topk) {
      return;
    }
  }

  for (; j < input_2_size; ++j) {
    auto& distance = document_with_scores_2->at(j);
    results.add_document_with_scores()->Swap(&distance);
    if (results.document_with_scores_size() >= topk) {
      return;
    }
  }
}

static void MergeSearchResults(uint32_t topk, std::vector<pb::document::DocumentWithScoreResult>& input_1,
                               std::vector<pb::document::DocumentWithScoreResult>& input_2,
                               std::vector<pb::document::DocumentWithScoreResult>& results) {
  assert(input_1.size() == input_2.size());

  results.resize(input_1.size());
  for (int i = 0; i < input_1.size(); ++i) {
    MergeSearchResult(topk, input_1[i], input_2[i], results[i]);
  }
}

butil::Status DocumentIndexWrapper::Search(std::vector<pb::common::DocumentWithId> document_with_ids, uint32_t topk,
                                           const pb::common::Range& region_range,
                                           std::vector<std::shared_ptr<DocumentIndex::FilterFunctor>>& filters,
                                           bool reconstruct, const pb::common::DocumentSearchParameter& parameter,
                                           std::vector<pb::document::DocumentWithScoreResult>& results) {
  if (!IsReady()) {
    DINGO_LOG(WARNING) << fmt::format("[vector_index.wrapper][index_id({})] vector index is not ready.", Id());
    return butil::Status(pb::error::EVECTOR_INDEX_NOT_FOUND, "vector index %lu is not ready.", Id());
  }
  auto document_index = GetDocumentIndex();
  if (document_index == nullptr) {
    DINGO_LOG(WARNING) << fmt::format("[vector_index.wrapper][index_id({})] vector index is not ready.", Id());
    return butil::Status(pb::error::EVECTOR_INDEX_NOT_FOUND, "vector index %lu is not ready.", Id());
  }

  // Exist sibling document index, so need to separate search document.
  auto sibling_document_index = SiblingDocumentIndex();
  if (sibling_document_index != nullptr) {
    std::vector<pb::document::DocumentWithScoreResult> results_1;
    auto status =
        sibling_document_index->SearchByParallel(document_with_ids, topk, filters, reconstruct, parameter, results_1);
    if (!status.ok()) {
      return status;
    }

    std::vector<pb::document::DocumentWithScoreResult> results_2;
    status = document_index->SearchByParallel(document_with_ids, topk, filters, reconstruct, parameter, results_2);
    if (!status.ok()) {
      return status;
    }

    MergeSearchResults(topk, results_1, results_2, results);
    return status;
  }

  const auto& index_range = document_index->Range();
  if (region_range.start_key() != index_range.start_key() || region_range.end_key() != index_range.end_key()) {
    int64_t min_document_id = 0, max_document_id = 0;
    DocumentCodec::DecodeRangeToDocumentId(region_range, min_document_id, max_document_id);
    auto ret =
        DocumentIndexWrapper::SetDocumentIndexRangeFilter(document_index, filters, min_document_id, max_document_id);
    if (!ret.ok()) {
      DINGO_LOG(ERROR) << fmt::format("[vector_index.wrapper][index_id({})] set vector index filter failed, error: {}",
                                      Id(), ret.error_str());
      return ret;
    }
  }

  return document_index->SearchByParallel(document_with_ids, topk, filters, reconstruct, parameter, results);
}

static void MergeRangeSearchResults(std::vector<pb::document::DocumentWithScoreResult>& input_1,
                                    std::vector<pb::document::DocumentWithScoreResult>& input_2,
                                    std::vector<pb::document::DocumentWithScoreResult>& results) {
  assert(input_1.size() == input_2.size());

  results.resize(input_1.size());
  for (int i = 0; i < input_1.size(); ++i) {
    MergeSearchResult(UINT32_MAX, input_1[i], input_2[i], results[i]);
  }
}

bool DocumentIndexWrapper::IsPermanentHoldDocumentIndex(int64_t region_id) {
  auto config = ConfigManager::GetInstance().GetRoleConfig();
  if (config == nullptr) {
    return true;
  }

  auto region = Server::GetInstance().GetRegion(region_id);
  if (region == nullptr) {
    DINGO_LOG(ERROR) << fmt::format("[vector_index.wrapper][index_id({})] Not found region.", region_id);
    return false;
  }
  if (region->GetStoreEngineType() == pb::common::STORE_ENG_MONO_STORE) {
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

butil::Status DocumentIndexWrapper::SetDocumentIndexRangeFilter(
    DocumentIndexPtr /*vector_index*/, std::vector<std::shared_ptr<DocumentIndex::FilterFunctor>>& filters,
    int64_t min_document_id, int64_t max_document_id) {
  filters.push_back(std::make_shared<DocumentIndex::RangeFilterFunctor>(min_document_id, max_document_id));
  return butil::Status::OK();
}

}  // namespace dingodb
