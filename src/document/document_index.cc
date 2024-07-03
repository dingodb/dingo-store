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

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "bthread/bthread.h"
#include "butil/status.h"
#include "common/constant.h"
#include "common/helper.h"
#include "common/logging.h"
#include "document/codec.h"
#include "document/document_index_snapshot_manager.h"
#include "fmt/core.h"
#include "proto/common.pb.h"
#include "proto/error.pb.h"
#include "server/server.h"
#include "tantivy_search.h"

namespace dingodb {

butil::Status DocumentIndex::RemoveIndexFiles(int64_t id, const std::string& index_path) {
  DINGO_LOG(INFO) << fmt::format("[document_index.raw][id({})] remove index files, path: {}", id, index_path);
  Helper::RemoveAllFileOrDirectory(index_path);
  return butil::Status::OK();
}

DocumentIndex::DocumentIndex(int64_t id, const std::string& index_path,
                             const pb::common::DocumentIndexParameter& document_index_parameter,
                             const pb::common::RegionEpoch& epoch, const pb::common::Range& range)
    : id(id),
      index_path(index_path),
      apply_log_id(0),
      snapshot_log_id(0),
      document_index_parameter(document_index_parameter),
      epoch(epoch),
      range(range) {
  DINGO_LOG(DEBUG) << fmt::format("[new.DocumentIndex][id({})]", id);
}

DocumentIndex::~DocumentIndex() {
  DINGO_LOG(INFO) << fmt::format("[delete.DocumentIndex][id({})]", id);

  RWLockReadGuard guard(&rw_lock_);

  auto bool_result = ffi_free_index_writer(index_path);
  if (!bool_result.result) {
    DINGO_LOG(ERROR) << fmt::format("[document_index.raw][id({})] free index writer failed, error: {}, error_msg: {}",
                                    id, bool_result.error_code, bool_result.error_msg.c_str());
  }
  bool_result = ffi_free_index_reader(index_path);
  if (!bool_result.result) {
    DINGO_LOG(ERROR) << fmt::format("[document_index.raw][id({})] free index reader failed, error: {}, error_msg: {}",
                                    id, bool_result.error_code, bool_result.error_msg.c_str());
  }

  if (is_destroyed_) {
    DINGO_LOG(INFO) << fmt::format("[document_index.raw][id({})] document index is destroyed, will remove all files",
                                   id);
    RemoveIndexFiles(id, index_path);
  }
}

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
  DINGO_LOG(INFO) << fmt::format("[document_index.raw][id({})] set epoch({}->{}) and range({}->{})", id,
                                 Helper::RegionEpochToString(this->epoch), Helper::RegionEpochToString(epoch),
                                 Helper::RangeToString(this->range), Helper::RangeToString(range));
  this->epoch = epoch;
  this->range = range;
}

void DocumentIndex::LockWrite() { rw_lock_.LockWrite(); }

void DocumentIndex::UnlockWrite() { rw_lock_.UnlockWrite(); }

butil::Status DocumentIndex::Upsert(const std::vector<pb::common::DocumentWithId>& document_with_ids,
                                    bool reload_reader) {
  if (document_with_ids.empty()) {
    return butil::Status::OK();
  }

  std::vector<int64_t> delete_ids;

  delete_ids.reserve(document_with_ids.size());
  for (const auto& document_with_id : document_with_ids) {
    delete_ids.push_back(document_with_id.id());
  }

  auto status = Delete(delete_ids);
  if (!status.ok()) {
    return status;
  }

  return Add(document_with_ids, reload_reader);
}

butil::Status DocumentIndex::Add(const std::vector<pb::common::DocumentWithId>& document_with_ids, bool reload_reader) {
  DINGO_LOG(INFO) << fmt::format("[document_index.raw][id({})] add document count({})", id, document_with_ids.size());

  if (document_with_ids.empty()) {
    return butil::Status::OK();
  }

  RWLockWriteGuard guard(&rw_lock_);

  if (is_destroyed_) {
    std::string err_msg = fmt::format("[document_index.raw][id({})] document index is destroyed", id);
    DINGO_LOG(ERROR) << err_msg;
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, err_msg);
  }

  for (const auto& document_with_id : document_with_ids) {
    std::vector<std::string> text_column_names;
    std::vector<std::string> text_column_docs;
    std::vector<std::string> i64_column_names;
    std::vector<std::int64_t> i64_column_docs;
    std::vector<std::string> f64_column_names;
    std::vector<double> f64_column_docs;
    std::vector<std::string> bytes_column_names;
    std::vector<std::string> bytes_column_docs;

    uint64_t document_id = document_with_id.id();

    const auto& document = document_with_id.document();
    for (const auto& [field_name, document_value] : document.document_data()) {
      switch (document_value.field_type()) {
        case pb::common::ScalarFieldType::STRING:
          text_column_names.push_back(field_name);
          text_column_docs.push_back(document_value.field_value().string_data());
          break;
        case pb::common::ScalarFieldType::INT64:
          i64_column_names.push_back(field_name);
          i64_column_docs.push_back(document_value.field_value().long_data());
          break;
        case pb::common::ScalarFieldType::DOUBLE:
          f64_column_names.push_back(field_name);
          f64_column_docs.push_back(document_value.field_value().double_data());
          break;
        case pb::common::ScalarFieldType::BYTES:
          bytes_column_names.push_back(field_name);
          bytes_column_docs.push_back(document_value.field_value().bytes_data());
          break;
        default:
          std::string err_msg = fmt::format("[document_index.raw][id({})] document_id: ({}) unknown field type({})", id,
                                            document_id, pb::common::ScalarFieldType_Name(document_value.field_type()));
          DINGO_LOG(ERROR) << err_msg;
          return butil::Status(pb::error::EILLEGAL_PARAMTETERS, err_msg);
          break;
      }
    }

    auto bool_result = ffi_index_multi_type_column_docs(index_path, document_id, text_column_names, text_column_docs,
                                                        i64_column_names, i64_column_docs, f64_column_names,
                                                        f64_column_docs, bytes_column_names, bytes_column_docs);
    if (!bool_result.result) {
      std::string err_msg =
          fmt::format("[document_index.raw][id({})] document_id: ({}) add failed, error: {}, error_msg: {}", id,
                      document_id, bool_result.error_code, bool_result.error_msg.c_str());
      DINGO_LOG(ERROR) << err_msg;
      return butil::Status(pb::error::EINTERNAL, err_msg);
    }
  }

  auto bool_result = ffi_index_writer_commit(index_path);
  if (!bool_result.result) {
    std::string err_msg = fmt::format("[document_index.raw][id({})] commit failed, error: {}, error_msg: {}", id,
                                      bool_result.error_code, bool_result.error_msg.c_str());
    DINGO_LOG(ERROR) << err_msg;
    return butil::Status(pb::error::EINTERNAL, err_msg);
  }

  if (reload_reader) {
    bool_result = ffi_index_reader_reload(index_path);
    if (!bool_result.result) {
      std::string err_msg = fmt::format("[document_index.raw][id({})] reload failed, error: {}, error_msg: {}", id,
                                        bool_result.error_code, bool_result.error_msg.c_str());
      DINGO_LOG(ERROR) << err_msg;
      return butil::Status(pb::error::EINTERNAL, err_msg);
    }
  }

  return butil::Status::OK();
}

butil::Status DocumentIndex::Delete(const std::vector<int64_t>& delete_ids) {
  DINGO_LOG(INFO) << fmt::format("[document_index.raw][id({})] delete document count({})", id, delete_ids.size());

  if (delete_ids.empty()) {
    return butil::Status::OK();
  }

  RWLockWriteGuard guard(&rw_lock_);

  if (is_destroyed_) {
    std::string err_msg = fmt::format("[document_index.raw][id({})] document index is destroyed", id);
    DINGO_LOG(ERROR) << err_msg;
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, err_msg);
  }

  std::vector<uint64_t> delete_ids_uint64;

  for (const auto& delete_id : delete_ids) {
    if (delete_id < 0) {
      std::string err_msg =
          fmt::format("[document_index.raw][id({})] delete failed, error: delete_id({}) < 0", id, delete_id);
      DINGO_LOG(ERROR) << err_msg;
      return butil::Status(pb::error::EILLEGAL_PARAMTETERS, err_msg);
    } else if (delete_id >= INT64_MAX) {
      std::string err_msg =
          fmt::format("[document_index.raw][id({})] delete failed, error: delete_id({}) >= INT64_MAX", id, delete_id);
      DINGO_LOG(ERROR) << err_msg;
      return butil::Status(pb::error::EILLEGAL_PARAMTETERS, err_msg);
    }

    delete_ids_uint64.push_back(delete_id);
  }

  auto bool_result = ffi_delete_row_ids(index_path, delete_ids_uint64);
  if (!bool_result.result) {
    std::string err_msg = fmt::format("[document_index.raw][id({})] delete failed, error: {}, error_msg: {}", id,
                                      bool_result.error_code, bool_result.error_msg.c_str());
    DINGO_LOG(ERROR) << err_msg;
    return butil::Status(pb::error::EINTERNAL, err_msg);
  }

  return butil::Status::OK();
}

butil::Status DocumentIndex::Search(uint32_t topk, const std::string& query_string, bool use_range_filter,
                                    int64_t start_id, int64_t end_id, bool use_id_filter,
                                    const std::vector<uint64_t>& alive_ids,
                                    const std::vector<std::string>& column_names,
                                    std::vector<pb::common::DocumentWithScore>& results) {
  RWLockReadGuard guard(&rw_lock_);

  if (is_destroyed_) {
    std::string err_msg = fmt::format("[document_index.raw][id({})] document index is destroyed", id);
    DINGO_LOG(ERROR) << err_msg;
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, err_msg);
  }

  if (topk == 0) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "topk must be greater than 0");
  }

  if (query_string.empty()) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "query string must not be empty");
  }

  // if (use_id_filter) {
  //   for (const auto& id : alive_ids) {
  //     if (id < 0 || id >= INT64_MAX) {
  //       return butil::Status(pb::error::EILLEGAL_PARAMTETERS,
  //                            "document id must be greater than 0 and lesser than INT64_MAX");
  //     }
  //   }
  // }

  auto search_result = ffi_bm25_search_with_column_names(index_path, query_string, topk, alive_ids, use_id_filter,
                                                         use_range_filter, start_id, end_id, column_names);

  if (search_result.error_code == 0) {
    for (const auto& row_id_with_score : search_result.result) {
      pb::common::DocumentWithScore result_doc;
      result_doc.mutable_document_with_id()->set_id(row_id_with_score.row_id);
      result_doc.set_score(row_id_with_score.score);
      results.push_back(result_doc);

      DINGO_LOG(INFO) << fmt::format("[document_index.raw][id({})] search result, row_id({}) score({})", id,
                                     row_id_with_score.row_id, row_id_with_score.score);
    }

    return butil::Status::OK();
  } else {
    std::string err_msg = fmt::format("[document_index.raw][id({})] search failed, error: {}, error_msg: {}", id,
                                      search_result.error_code, search_result.error_msg.c_str());
    DINGO_LOG(ERROR) << err_msg;
    return butil::Status(pb::error::EINTERNAL, err_msg);
  }
}

butil::Status DocumentIndex::Save(const std::string& /*path*/) {
  // Save need the caller to do LockWrite() and UnlockWrite()
  auto result = ffi_index_writer_commit(index_path);
  if (result.result) {
    return butil::Status::OK();
  } else {
    std::string err_msg = fmt::format("[document_index.raw][id({})] save failed, error: {}, error_msg: {}", id,
                                      result.error_code, result.error_msg.c_str());
    DINGO_LOG(ERROR) << err_msg;
    return butil::Status(pb::error::EINTERNAL, err_msg);
  }
}

butil::Status DocumentIndex::Load(const std::string& /*path*/) {
  auto result = ffi_index_reader_reload(index_path);
  if (result.result) {
    return butil::Status::OK();
  } else {
    std::string err_msg = fmt::format("[document_index.raw][id({})] load failed, error: {}, error_msg: {}", id,
                                      result.error_code, result.error_msg.c_str());
    DINGO_LOG(ERROR) << err_msg;
    return butil::Status(pb::error::EINTERNAL, err_msg);
  }
}

butil::Status DocumentIndex::GetDocCount(int64_t& count) {
  RWLockReadGuard guard(&rw_lock_);
  if (is_destroyed_) {
    std::string err_msg = fmt::format("[document_index.raw][id({})] document index is destroyed", id);
    DINGO_LOG(ERROR) << err_msg;
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, err_msg);
  }

  count = ffi_get_indexed_doc_counts(index_path);
  return butil::Status::OK();
}

butil::Status DocumentIndex::GetTokenCount(int64_t& count) {
  RWLockReadGuard guard(&rw_lock_);
  if (is_destroyed_) {
    std::string err_msg = fmt::format("[document_index.raw][id({})] document index is destroyed", id);
    DINGO_LOG(ERROR) << err_msg;
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, err_msg);
  }

  count = ffi_get_total_num_tokens(index_path);
  return butil::Status::OK();
}

butil::Status DocumentIndex::GetMetaJson(std::string& json) {
  RWLockReadGuard guard(&rw_lock_);
  if (is_destroyed_) {
    std::string err_msg = fmt::format("[document_index.raw][id({})] document index is destroyed", id);
    DINGO_LOG(ERROR) << err_msg;
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, err_msg);
  }

  auto string_result = ffi_get_index_meta_json(index_path);
  if (string_result.error_code != 0) {
    std::string err_msg = fmt::format("ffi_get_index_meta_json faild for ({}), error_code: ({}), error_msg: ({})",
                                      index_path, string_result.error_code, string_result.error_msg.c_str());
    DINGO_LOG(ERROR) << err_msg;
    return butil::Status(pb::error::EINTERNAL, err_msg);
  }

  json = std::string(string_result.result.c_str());

  return butil::Status::OK();
}

butil::Status DocumentIndex::GetJsonParameter(std::string& json) {
  RWLockReadGuard guard(&rw_lock_);
  if (is_destroyed_) {
    std::string err_msg = fmt::format("[document_index.raw][id({})] document index is destroyed", id);
    DINGO_LOG(ERROR) << err_msg;
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, err_msg);
  }

  auto string_result = ffi_get_index_json_parameter(index_path);
  if (string_result.error_code != 0) {
    std::string err_msg = fmt::format("ffi_get_index_json_parameter faild for ({}), error_code: ({}), error_msg: ({})",
                                      index_path, string_result.error_code, string_result.error_msg.c_str());
    DINGO_LOG(ERROR) << err_msg;
    return butil::Status(pb::error::EINTERNAL, err_msg);
  }

  json = std::string(string_result.result.c_str());

  return butil::Status::OK();
}

DocumentIndexWrapper::DocumentIndexWrapper(int64_t id, pb::common::DocumentIndexParameter index_parameter,
                                           int64_t save_snapshot_threshold_write_key_num)
    : id_(id),
      ready_(false),
      destoryed_(false),
      is_switching_document_index_(false),
      apply_log_id_(0),
      snapshot_log_id_(0),
      index_parameter_(index_parameter),
      pending_task_num_(0),
      loadorbuilding_num_(0),
      rebuilding_num_(0),
      saving_num_(0),
      save_snapshot_threshold_write_key_num_(save_snapshot_threshold_write_key_num) {
  bthread_mutex_init(&document_index_mutex_, nullptr);
  DINGO_LOG(DEBUG) << fmt::format("[new.DocumentIndexWrapper][id({})]", id_);
}

DocumentIndexWrapper::~DocumentIndexWrapper() {
  ClearDocumentIndex("destruct");

  bthread_mutex_destroy(&document_index_mutex_);
  DINGO_LOG(INFO) << fmt::format("[delete.DocumentIndexWrapper][id({})]", id_);

  if (IsDestoryed()) {
    // remove document index dir from disk
    auto region_document_index_path = DocumentIndexSnapshotManager::GetSnapshotParentPath(id_);
    if (!region_document_index_path.empty()) {
      auto ret = Helper::RemoveAllFileOrDirectory(region_document_index_path);
      if (!ret) {
        DINGO_LOG(ERROR) << fmt::format(
            "[document_index.wrapper][index_id({})] remove document index dir failed, dir: ({}).", Id(),
            region_document_index_path);
      } else {
        DINGO_LOG(INFO) << fmt::format(
            "[document_index.wrapper][index_id({})] remove document index dir success, dir: ({}).", Id(),
            region_document_index_path);
      }
    } else {
      DINGO_LOG(ERROR) << fmt::format("[document_index.wrapper][index_id({})] get document index dir failed.", Id());
    }

    RemoveMeta();
  }
}

std::shared_ptr<DocumentIndexWrapper> DocumentIndexWrapper::New(int64_t id,
                                                                pb::common::DocumentIndexParameter index_parameter) {
  auto document_index_wrapper = std::make_shared<DocumentIndexWrapper>(
      id, index_parameter, Constant::kDocumentIndexSaveSnapshotThresholdWriteKeyNum);
  if (document_index_wrapper != nullptr) {
    if (!document_index_wrapper->Init()) {
      return nullptr;
    }
  }

  return document_index_wrapper;
}

std::shared_ptr<DocumentIndexWrapper> DocumentIndexWrapper::GetSelf() { return shared_from_this(); }

bool DocumentIndexWrapper::Init() { return true; }  // NOLINT

void DocumentIndexWrapper::Destroy() {
  DINGO_LOG(INFO) << fmt::format("[document_index.wrapper][index_id({})] document index destroy.", Id());
  destoryed_.store(true);

  // destory document index
  auto own_index = GetOwnDocumentIndex();
  if (own_index != nullptr) {
    own_index->SetDestroyed();
  }
}

bool DocumentIndexWrapper::Recover() {
  auto status = LoadMeta();
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("[document_index.wrapper][index_id({})] document index recover failed, error: {}",
                                    Id(), status.error_str());
    return false;
  }

  return true;
}

static std::string GenMetaKeyDocument(int64_t document_index_id) {
  return fmt::format("{}_{}", Constant::kDocumentIndexApplyLogIdPrefix, document_index_id);
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

  auto kv = std::make_shared<pb::common::KeyValue>();
  kv->set_key(GenMetaKeyDocument(id_));
  kv->set_value(meta.SerializeAsString());
  if (!meta_writer->Put(kv)) {
    return butil::Status(pb::error::EINTERNAL, "Write document index meta failed.");
  }

  return butil::Status();
}

butil::Status DocumentIndexWrapper::RemoveMeta() const {
  auto meta_writer = Server::GetInstance().GetMetaWriter();
  if (meta_writer == nullptr) {
    return butil::Status(pb::error::EINTERNAL, "meta writer is nullptr.");
  }

  if (!meta_writer->Delete(GenMetaKeyDocument(id_))) {
    DINGO_LOG(ERROR) << fmt::format("[document_index.wrapper][index_id({})] delete document index meta failed.", Id());
    return butil::Status(pb::error::EINTERNAL, "Delete document index meta failed.");
  }

  return butil::Status();
}

butil::Status DocumentIndexWrapper::LoadMeta() {
  auto meta_reader = Server::GetInstance().GetMetaReader();
  if (meta_reader == nullptr) {
    return butil::Status(pb::error::EINTERNAL, "meta reader is nullptr.");
  }

  auto kv = meta_reader->Get(GenMetaKeyDocument(id_));
  if (kv == nullptr) {
    return butil::Status(pb::error::EINTERNAL, "Get document index meta failed");
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
    DINGO_LOG(WARNING) << fmt::format("[document_index.wrapper][index_id({})] prase document index meta failed.", Id());
  }

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

bool DocumentIndexWrapper::IsSwitchingDocumentIndex() { return is_switching_document_index_.load(); }

void DocumentIndexWrapper::SetIsSwitchingDocumentIndex(bool is_switching) {
  is_switching_document_index_.store(is_switching);
}

void DocumentIndexWrapper::UpdateDocumentIndex(DocumentIndexPtr document_index, const std::string& trace) {
  DINGO_LOG(INFO) << fmt::format(
      "[document_index.wrapper][index_id({})][trace({})] update document index, epoch({}) range({})", Id(), trace,
      Helper::RegionEpochToString(document_index->Epoch()),
      DocumentCodec::DecodeRangeToString(document_index->Range()));
  // Check document index is stop
  if (IsDestoryed()) {
    DINGO_LOG(WARNING) << fmt::format("[document_index.wrapper][index_id({})] document index is stop.", Id());
    return;
  }

  {
    BAIDU_SCOPED_LOCK(document_index_mutex_);

    auto old_index = document_index_;

    // for document index, after update, the old index should be destroyed explicitly
    if (old_index != nullptr) {
      old_index->SetDestroyed();
    }

    document_index_ = document_index;

    share_document_index_ = nullptr;

    if (sibling_document_index_ != nullptr &&
        Helper::IsContainRange(document_index->Range(), sibling_document_index_->Range())) {
      sibling_document_index_ = nullptr;
    }

    ++version_;

    ready_.store(true);

    int64_t apply_log_id = ApplyLogId();
    int64_t snapshot_log_id = SnapshotLogId();
    int64_t apply_log_id_new = document_index->ApplyLogId();

    DINGO_LOG(INFO) << fmt::format(
        "[document_index.wrapper][index_id({})][trace({})] update document index, apply_log_id({}/{}) "
        "snapshot_log_id({}/{}).",
        Id(), trace, apply_log_id, apply_log_id_new, snapshot_log_id, document_index->SnapshotLogId());
    if (apply_log_id < apply_log_id_new) {
      SetApplyLogId(apply_log_id_new);
    }
    if (snapshot_log_id < document_index->SnapshotLogId()) {
      SetSnapshotLogId(document_index->SnapshotLogId());
    }

    SaveMeta();
  }
}

void DocumentIndexWrapper::ClearDocumentIndex(const std::string& trace) {
  DINGO_LOG(INFO) << fmt::format("[document_index.wrapper][index_id({})][trace({})] Clear all document index", Id(),
                                 trace);

  BAIDU_SCOPED_LOCK(document_index_mutex_);

  ready_.store(false);

  if (document_index_ != nullptr) {
    document_index_->SetDestroyed();
  }

  document_index_ = nullptr;
  share_document_index_ = nullptr;
  sibling_document_index_ = nullptr;
}

DocumentIndexPtr DocumentIndexWrapper::GetOwnDocumentIndex() {
  BAIDU_SCOPED_LOCK(document_index_mutex_);
  return document_index_;
}

DocumentIndexPtr DocumentIndexWrapper::GetDocumentIndex() {
  BAIDU_SCOPED_LOCK(document_index_mutex_);

  if (share_document_index_ != nullptr) {
    return share_document_index_;
  }

  return document_index_;
}

DocumentIndexPtr DocumentIndexWrapper::ShareDocumentIndex() {
  BAIDU_SCOPED_LOCK(document_index_mutex_);
  return share_document_index_;
}

void DocumentIndexWrapper::SetShareDocumentIndex(DocumentIndexPtr document_index) {
  BAIDU_SCOPED_LOCK(document_index_mutex_);

  share_document_index_ = document_index;
  if (share_document_index_ != nullptr) {
    share_document_index_->SetDestroyed();
  }

  // During split, there may occur leader change, set ready_ to true can improve the availablidy of document index
  // Because follower is also do force rebuild too, so in this scenario follower is equivalent to leader
  ready_.store(true);
}

DocumentIndexPtr DocumentIndexWrapper::SiblingDocumentIndex() {
  BAIDU_SCOPED_LOCK(document_index_mutex_);
  return sibling_document_index_;
}

void DocumentIndexWrapper::SetSiblingDocumentIndex(DocumentIndexPtr document_index) {
  BAIDU_SCOPED_LOCK(document_index_mutex_);
  sibling_document_index_ = document_index;
  if (sibling_document_index_ != nullptr) {
    sibling_document_index_->SetDestroyed();
  }
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

butil::Status DocumentIndexWrapper::GetDocCount(int64_t& count) {
  auto document_index = GetOwnDocumentIndex();
  if (document_index == nullptr) {
    return butil::Status(pb::error::EDOCUMENT_INDEX_NOT_FOUND, "document index %lu is not ready.", Id());
  }

  int64_t own_count = 0;
  auto status = document_index->GetDocCount(own_count);
  if (!status.ok()) {
    return status;
  }

  int64_t sibling_count = 0;
  auto sibling_document_index = SiblingDocumentIndex();
  if (sibling_document_index != nullptr) {
    status = sibling_document_index->GetDocCount(sibling_count);
    if (!status.ok()) {
      return status;
    }
  }

  count = own_count + sibling_count;
  return status;
}

butil::Status DocumentIndexWrapper::GetTokenCount(int64_t& count) {
  auto document_index = GetOwnDocumentIndex();
  if (document_index == nullptr) {
    return butil::Status(pb::error::EDOCUMENT_INDEX_NOT_FOUND, "document index %lu is not ready.", Id());
  }

  int64_t own_count = 0;
  auto status = document_index->GetTokenCount(own_count);
  if (!status.ok()) {
    return status;
  }

  int64_t sibling_count = 0;
  auto sibling_document_index = SiblingDocumentIndex();
  if (sibling_document_index != nullptr) {
    status = sibling_document_index->GetTokenCount(sibling_count);
    if (!status.ok()) {
      return status;
    }
  }

  count = own_count + sibling_count;
  return status;
}

butil::Status DocumentIndexWrapper::GetMetaJson(std::string& json) {
  auto document_index = GetOwnDocumentIndex();
  if (document_index == nullptr) {
    return butil::Status(pb::error::EDOCUMENT_INDEX_NOT_FOUND, "document index %lu is not ready.", Id());
  }

  return document_index->GetMetaJson(json);
}

butil::Status DocumentIndexWrapper::GetJsonParameter(std::string& json) {
  auto document_index = GetOwnDocumentIndex();
  if (document_index == nullptr) {
    return butil::Status(pb::error::EDOCUMENT_INDEX_NOT_FOUND, "document index %lu is not ready.", Id());
  }

  return document_index->GetJsonParameter(json);
}

bool DocumentIndexWrapper::NeedToRebuild() {
  auto document_index = GetOwnDocumentIndex();
  if (document_index == nullptr) {
    return false;
  }

  return document_index->NeedToRebuild();
}

bool DocumentIndexWrapper::SupportSave() {
  auto document_index = GetOwnDocumentIndex();
  if (document_index == nullptr) {
    return false;
  }

  return document_index->SupportSave();
}

bool DocumentIndexWrapper::NeedToSave(std::string& reason) {
  auto document_index = GetOwnDocumentIndex();
  if (document_index == nullptr) {
    return false;
  }

  if (Helper::InvalidRange(document_index->Range())) {
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
  bool ret = document_index->NeedToSave(last_save_log_behind);
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
      "[document_index.wrapper][index_id({})] not need save, last_save_log_behind={} write_key_count={}/{}/{}", Id(),
      last_save_log_behind, write_key_count_, last_save_write_key_count_, save_snapshot_threshold_write_key_num_);

  return false;
}

// Filter document id by range
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

// Filter document id by range
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

butil::Status DocumentIndexWrapper::Upsert(const std::vector<pb::common::DocumentWithId>& document_with_ids) {
  if (!IsReady()) {
    DINGO_LOG(WARNING) << fmt::format("[document_index.wrapper][index_id({})] document index is not ready.", Id());
    return butil::Status(pb::error::EDOCUMENT_INDEX_NOT_FOUND, "document index %lu is not ready.", Id());
  }

  // Waiting switch document index
  int count = 0;
  while (IsSwitchingDocumentIndex()) {
    DINGO_LOG(INFO) << fmt::format("[document_index.wrapper][index_id({})] waiting document index switch, count({})",
                                   Id(), ++count);
    bthread_usleep(1000 * 100);
  }

  auto document_index = GetDocumentIndex();
  if (document_index == nullptr) {
    DINGO_LOG(WARNING) << fmt::format("[document_index.wrapper][index_id({})] document index is not ready.", Id());
    return butil::Status(pb::error::EDOCUMENT_INDEX_NOT_FOUND, "document index %lu is not ready.", Id());
  }

  // Exist sibling document index, so need to separate add document.
  auto sibling_document_index = SiblingDocumentIndex();
  if (sibling_document_index != nullptr) {
    auto status =
        sibling_document_index->Upsert(FilterDocumentWithId(document_with_ids, sibling_document_index->Range()), true);
    if (!status.ok()) {
      return status;
    }

    status = document_index->Upsert(FilterDocumentWithId(document_with_ids, document_index->Range()), true);
    if (!status.ok()) {
      sibling_document_index->Delete(FilterDocumentId(document_with_ids, sibling_document_index->Range()));
      return status;
    }

    write_key_count_ += document_with_ids.size();

    return status;
  }

  auto status = document_index->Upsert(document_with_ids, true);
  if (status.ok()) {
    write_key_count_ += document_with_ids.size();
  }
  return status;
}

butil::Status DocumentIndexWrapper::Add(const std::vector<pb::common::DocumentWithId>& document_with_ids) {
  if (!IsReady()) {
    DINGO_LOG(WARNING) << fmt::format("[document_index.wrapper][index_id({})] document index is not ready.", Id());
    return butil::Status(pb::error::EDOCUMENT_INDEX_NOT_FOUND, "document index %lu is not ready.", Id());
  }

  // Waiting switch document index
  int count = 0;
  while (IsSwitchingDocumentIndex()) {
    DINGO_LOG(INFO) << fmt::format("[document_index.wrapper][index_id({})] waiting document index switch, count({})",
                                   Id(), ++count);
    bthread_usleep(1000 * 100);
  }

  auto document_index = GetDocumentIndex();
  if (document_index == nullptr) {
    DINGO_LOG(WARNING) << fmt::format("[document_index.wrapper][index_id({})] document index is not ready.", Id());
    return butil::Status(pb::error::EDOCUMENT_INDEX_NOT_FOUND, "document index %lu is not ready.", Id());
  }

  // Exist sibling document index, so need to separate add document.
  auto sibling_document_index = SiblingDocumentIndex();
  if (sibling_document_index != nullptr) {
    auto status =
        sibling_document_index->Add(FilterDocumentWithId(document_with_ids, sibling_document_index->Range()), true);
    if (!status.ok()) {
      return status;
    }

    status = document_index->Add(FilterDocumentWithId(document_with_ids, document_index->Range()), true);
    if (!status.ok()) {
      sibling_document_index->Delete(FilterDocumentId(document_with_ids, sibling_document_index->Range()));
      return status;
    }

    write_key_count_ += document_with_ids.size();

    return status;
  }

  auto status = document_index->Add(document_with_ids, true);
  if (status.ok()) {
    write_key_count_ += document_with_ids.size();
  }
  return status;
}

butil::Status DocumentIndexWrapper::Delete(const std::vector<int64_t>& delete_ids) {
  if (!IsReady()) {
    DINGO_LOG(WARNING) << fmt::format("[document_index.wrapper][index_id({})] document index is not ready.", Id());
    return butil::Status(pb::error::EDOCUMENT_INDEX_NOT_FOUND, "document index %lu is not ready.", Id());
  }

  // Switch document index wait
  int count = 0;
  while (IsSwitchingDocumentIndex()) {
    DINGO_LOG(INFO) << fmt::format("[document_index.wrapper][index_id({})] document index switch waiting, count({})",
                                   Id(), ++count);
    bthread_usleep(1000 * 100);
  }

  auto document_index = GetDocumentIndex();
  if (document_index == nullptr) {
    DINGO_LOG(WARNING) << fmt::format("[document_index.wrapper][index_id({})] document index is not ready.", Id());
    return butil::Status(pb::error::EDOCUMENT_INDEX_NOT_FOUND, "document index %lu is not ready.", Id());
  }

  // Exist sibling document index, so need to separate delete document.
  auto sibling_document_index = SiblingDocumentIndex();
  if (sibling_document_index != nullptr) {
    auto status = sibling_document_index->Delete(FilterDocumentId(delete_ids, sibling_document_index->Range()));
    if (!status.ok()) {
      return status;
    }

    status = document_index->Delete(FilterDocumentId(delete_ids, document_index->Range()));
    if (status.ok()) {
      write_key_count_ += delete_ids.size();
    }
    return status;
  }

  auto status = document_index->Delete(delete_ids);
  if (status.ok()) {
    write_key_count_ += delete_ids.size();
  }
  return status;
}

static void MergeSearchResult(uint32_t topk, std::vector<pb::common::DocumentWithScore>& input_1,
                              std::vector<pb::common::DocumentWithScore>& input_2,
                              std::vector<pb::common::DocumentWithScore>& results) {
  if (topk == 0) return;
  int input_1_size = input_1.size();
  int input_2_size = input_2.size();
  const auto& document_with_scores_1 = input_1;
  const auto& document_with_scores_2 = input_2;

  int i = 0, j = 0;

  // for document, the bigger score mean more relative.
  while (i < input_1_size && j < input_2_size) {
    const auto& score_1 = document_with_scores_1.at(i);
    const auto& score_2 = document_with_scores_2.at(j);
    if (score_1.score() > score_2.score()) {
      ++i;
      results.push_back(score_1);
    } else {
      ++j;
      results.push_back(score_2);
    }

    if (results.size() >= topk) {
      return;
    }
  }

  for (; i < input_1_size; ++i) {
    const auto& distance = document_with_scores_1.at(i);
    results.push_back(distance);
    if (results.size() >= topk) {
      return;
    }
  }

  for (; j < input_2_size; ++j) {
    const auto& distance = document_with_scores_2.at(j);
    results.push_back(distance);
    if (results.size() >= topk) {
      return;
    }
  }
}

butil::Status DocumentIndexWrapper::Search(const pb::common::Range& region_range,
                                           const pb::common::DocumentSearchParameter& parameter,
                                           std::vector<pb::common::DocumentWithScore>& results) {
  if (!IsReady()) {
    DINGO_LOG(WARNING) << fmt::format("[document_index.wrapper][index_id({})] document index is not ready.", Id());
    return butil::Status(pb::error::EDOCUMENT_INDEX_NOT_FOUND, "document index %lu is not ready.", Id());
  }
  auto document_index = GetDocumentIndex();
  if (document_index == nullptr) {
    DINGO_LOG(WARNING) << fmt::format("[document_index.wrapper][index_id({})] document index is not ready.", Id());
    return butil::Status(pb::error::EDOCUMENT_INDEX_NOT_FOUND, "document index %lu is not ready.", Id());
  }

  bool use_id_filter = false;
  if (!parameter.document_ids().empty()) {
    use_id_filter = true;
  }

  std::vector<uint64_t> alive_ids;
  for (int64_t doc_id : parameter.document_ids()) {
    alive_ids.push_back(doc_id);
  }

  std::vector<std::string> column_names;
  for (const auto& column_name : parameter.column_names()) {
    column_names.push_back(column_name);
  }

  // Exist sibling document index, so need to separate search document.
  auto sibling_document_index = SiblingDocumentIndex();
  if (sibling_document_index != nullptr) {
    DINGO_LOG(INFO) << fmt::format("[document_index.wrapper][index_id({})] search document in sibling document index.",
                                   Id());
    std::vector<pb::common::DocumentWithScore> results_1;
    auto status = sibling_document_index->Search(parameter.top_n(), parameter.query_string(), false, 0, INT64_MAX,
                                                 use_id_filter, alive_ids, column_names, results_1);
    if (!status.ok()) {
      return status;
    }

    std::vector<pb::common::DocumentWithScore> results_2;
    status = document_index->Search(parameter.top_n(), parameter.query_string(), false, 0, INT64_MAX, use_id_filter,
                                    alive_ids, column_names, results_2);
    if (!status.ok()) {
      return status;
    }

    MergeSearchResult(parameter.top_n(), results_1, results_2, results);
    return status;
  }

  const auto& index_range = document_index->Range();
  if (region_range.start_key() != index_range.start_key() || region_range.end_key() != index_range.end_key()) {
    int64_t min_document_id = 0, max_document_id = 0;
    DocumentCodec::DecodeRangeToDocumentId(region_range, min_document_id, max_document_id);

    DINGO_LOG(INFO) << fmt::format(
        "[document_index.wrapper][index_id({})] search document in document index with range_filter, range({}) "
        "query_string({}) "
        "top_n({}) min_document_id({}) max_document_id({})",
        Id(), DocumentCodec::DecodeRangeToString(region_range), parameter.query_string(), parameter.top_n(),
        min_document_id, max_document_id);

    // use range filter
    return document_index->Search(parameter.top_n(), parameter.query_string(), true, min_document_id, max_document_id,
                                  use_id_filter, alive_ids, column_names, results);

    // auto ret =
    //     DocumentIndexWrapper::SetDocumentIndexRangeFilter(document_index, filters, min_document_id,
    //     max_document_id);
    // if (!ret.ok()) {
    //   DINGO_LOG(ERROR) << fmt::format(
    //       "[document_index.wrapper][index_id({})] set document index filter failed, error: {}", Id(),
    //       ret.error_str());
    //   return ret;
    // }
  }

  DINGO_LOG(INFO) << fmt::format(
      "[document_index.wrapper][index_id({})] search document in document index, range({}) query_string({}) top_n({})",
      Id(), DocumentCodec::DecodeRangeToString(region_range), parameter.query_string(), parameter.top_n());

  return document_index->Search(parameter.top_n(), parameter.query_string(), false, 0, INT64_MAX, use_id_filter,
                                alive_ids, column_names, results);
}

// butil::Status DocumentIndexWrapper::SetDocumentIndexRangeFilter(
//     DocumentIndexPtr /*document_index*/, std::vector<std::shared_ptr<DocumentIndex::FilterFunctor>>& filters,
//     int64_t min_document_id, int64_t max_document_id) {
//   filters.push_back(std::make_shared<DocumentIndex::RangeFilterFunctor>(min_document_id, max_document_id));
//   return butil::Status::OK();
// }

// bool DocumentIndexWrapper::IsTempHoldDocumentIndex() const { return is_hold_document_index_.load(); }

// void DocumentIndexWrapper::SetIsTempHoldDocumentIndex(bool need) {
//   DINGO_LOG(INFO) << fmt::format("[document_index.wrapper][index_id({})] set document index hold({}->{})", Id(),
//                                  IsTempHoldDocumentIndex(), need);
//   is_hold_document_index_.store(need);
//   SaveMeta();
// }

// For document index, all node need to hold the index, so this function always return true.
bool DocumentIndexWrapper::IsPermanentHoldDocumentIndex(int64_t /*region_id*/) {
  // auto config = ConfigManager::GetInstance().GetRoleConfig();
  // if (config == nullptr) {
  //   return true;
  // }

  // auto region = Server::GetInstance().GetRegion(region_id);
  // if (region == nullptr) {
  //   DINGO_LOG(ERROR) << fmt::format("[document_index.wrapper][index_id({})] Not found region.", region_id);
  //   return false;
  // }
  // if (region->GetStoreEngineType() == pb::common::STORE_ENG_MONO_STORE) {
  //   return true;
  // }

  // if (!config->GetBool("document.enable_follower_hold_index")) {
  //   // If follower, delete document index.
  //   if (!Server::GetInstance().IsLeader(region_id)) {
  //     return false;
  //   }
  // }
  return true;
}

}  // namespace dingodb
