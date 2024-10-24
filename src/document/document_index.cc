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

#include <atomic>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "braft/protobuf_file.h"
#include "bthread/bthread.h"
#include "butil/status.h"
#include "common/constant.h"
#include "common/helper.h"
#include "common/logging.h"
#include "document/codec.h"
#include "document/document_index_factory.h"
#include "fmt/core.h"
#include "mvcc/codec.h"
#include "proto/common.pb.h"
#include "proto/error.pb.h"
#include "server/server.h"
#include "tantivy_search.h"

namespace dingodb {

DEFINE_int32(document_index_save_log_gap, 10, "document index save log gap");
BRPC_VALIDATE_GFLAG(document_index_save_log_gap, brpc::PositiveInteger);

butil::Status DocumentIndex::RemoveIndexFiles(int64_t id, const std::string& index_path) {
  // index_path: /home/dingo-store/dist/document1/data/document_index/80040/epoch_1
  // need remove index_path: /home/dingo-store/dist/document1/data/document_index/80040
  size_t last_slash = index_path.find_last_of('/');
  if (last_slash == std::string::npos) {
    auto s =
        fmt::format("[document_index.raw][id({})] remove index file failed, path: {} not find '/' ", id, index_path);
    DINGO_LOG(ERROR) << s;
    return butil::Status(pb::error::Errno::EFILE_NOT_DIRECTORY, s);
  }
  std::string remove_path = index_path.substr(0, last_slash);
  DINGO_LOG(INFO) << fmt::format("[document_index.raw][id({})] remove index files, path: {}", id, remove_path);

  Helper::RemoveAllFileOrDirectory(remove_path);
  return butil::Status::OK();
}

DocumentIndex::DocumentIndex(int64_t id, const std::string& index_path,
                             const pb::common::DocumentIndexParameter& document_index_parameter,
                             const pb::common::RegionEpoch& epoch, const pb::common::Range& range)
    : id_(id),
      index_path_(index_path),
      apply_log_id_(0),
      document_index_parameter_(document_index_parameter),
      epoch_(epoch),
      range_(range) {}

DocumentIndex::~DocumentIndex() {
  RWLockReadGuard guard(&rw_lock_);

  auto bool_result = ffi_free_index_writer(index_path_);
  if (!bool_result.result) {
    DINGO_LOG(ERROR) << fmt::format("[document_index.raw][id({})] free index writer failed, error: {}, error_msg: {}",
                                    id_, bool_result.error_code, bool_result.error_msg.c_str());
  }
  bool_result = ffi_free_index_reader(index_path_);
  if (!bool_result.result) {
    DINGO_LOG(ERROR) << fmt::format("[document_index.raw][id({})] free index reader failed, error: {}, error_msg: {}",
                                    id_, bool_result.error_code, bool_result.error_msg.c_str());
  }

  if (is_destroyed_) {
    DINGO_LOG(INFO) << fmt::format("[document_index.raw][id({})] document index is destroyed, will remove all files",
                                   id_);
    RemoveIndexFiles(id_, index_path_);
  }
}

int64_t DocumentIndex::ApplyLogId() const { return apply_log_id_.load(std::memory_order_relaxed); }

void DocumentIndex::SetApplyLogId(int64_t apply_log_id) {
  this->apply_log_id_.store(apply_log_id, std::memory_order_relaxed);
}

pb::common::RegionEpoch DocumentIndex::Epoch() const { return epoch_; };

pb::common::Range DocumentIndex::Range(bool is_encode) const {
  return is_encode ? mvcc::Codec::EncodeRange(range_) : range_;
}

std::string DocumentIndex::RangeString() const { return DocumentCodec::DebugRange(false, range_); }

void DocumentIndex::SetEpochAndRange(const pb::common::RegionEpoch& epoch, const pb::common::Range& range) {
  DINGO_LOG(INFO) << fmt::format("[document_index.raw][id({})] set epoch({}->{}) and range({}->{})", id_,
                                 Helper::RegionEpochToString(epoch_), Helper::RegionEpochToString(epoch),
                                 Helper::RangeToString(range_), Helper::RangeToString(range));
  epoch_ = epoch;
  range_ = range;
}

void DocumentIndex::LockWrite() { rw_lock_.LockWrite(); }

void DocumentIndex::UnlockWrite() { rw_lock_.UnlockWrite(); }

butil::Status DocumentIndex::SaveMeta(int64_t apply_log_id) {
  LockWrite();

  SetApplyLogId(apply_log_id);

  // Write meta to meta_file
  pb::store_internal::DocumentIndexSnapshotMeta meta;
  meta.set_document_index_id(id_);
  meta.set_apply_log_id(apply_log_id);
  *(meta.mutable_range()) = Range(false);
  *(meta.mutable_epoch()) = Epoch();

  std::string meta_filepath = fmt::format("{}/meta", IndexPath());

  braft::ProtoBufFile pb_file_meta(meta_filepath);
  if (pb_file_meta.save(&meta, true) != 0) {
    DINGO_LOG(ERROR) << fmt::format("[document_index.raw][id({})] save meta file fail.", id_);
    return butil::Status(pb::error::EINTERNAL, "save meta fail");
  }

  UnlockWrite();

  return butil::Status::OK();
}

std::string DocumentIndex::GetIndexPath(int64_t document_index_id, const pb::common::RegionEpoch& epoch) {
  return fmt::format("{}/{}/epoch_{}", Server::GetInstance().GetDocumentIndexPath(), document_index_id,
                     epoch.version());
}

std::shared_ptr<DocumentIndex> DocumentIndex::LoadIndex(int64_t id, const pb::common::RegionEpoch& epoch,
                                                        const pb::common::DocumentIndexParameter& param) {
  auto path = GetIndexPath(id, epoch);

  if (!Helper::IsExistPath(path)) {
    DINGO_LOG(ERROR) << fmt::format("[document_index.raw][id({})] not found index dir, path: {}.", id, path);
    return nullptr;
  }

  auto meta_path = fmt::format("{}/meta", path);
  if (!Helper::IsExistPath(meta_path)) {
    DINGO_LOG(ERROR) << fmt::format("[document_index.raw][id({})] not found meta file, path: {}.", id, meta_path);
    return nullptr;
  }

  pb::store_internal::DocumentIndexSnapshotMeta meta;
  braft::ProtoBufFile pb_file_meta(meta_path);
  if (pb_file_meta.load(&meta) != 0) {
    DINGO_LOG(ERROR) << fmt::format("[document_index.raw][id({})] load meta fail, index_path: {}", id, path);
    return nullptr;
  }

  if (meta.document_index_id() != id) {
    DINGO_LOG(ERROR) << fmt::format("[document_index.raw][id({})] load meta fail, id not match, path: {}.", id, path);
    return nullptr;
  }

  if (meta.epoch().version() != epoch.version()) {
    DINGO_LOG(ERROR) << fmt::format(
        "[document_index.raw][id({})] load meta fail, epoch version not match, path: {} version({}/{}).", id, path,
        meta.epoch().version(), epoch.version());
    return nullptr;
  }

  auto document_index = DocumentIndexFactory::LoadIndex(id, path, param, meta.epoch(), meta.range());
  if (document_index == nullptr) {
    DINGO_LOG(ERROR) << fmt::format("[document_index.raw][id({})] load meta fail, index_path: {}", id, path);
    return nullptr;
  }

  document_index->SetApplyLogId(meta.apply_log_id());

  DINGO_LOG(INFO) << fmt::format("[document_index.raw][id({})] load meta finish, epoch({}) apply_id({})", id,
                                 meta.epoch().version(), meta.apply_log_id());

  return document_index;
}

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
  DINGO_LOG(DEBUG) << fmt::format("[document_index.raw][id({})] add document count({})", id_, document_with_ids.size());

  if (document_with_ids.empty()) {
    return butil::Status::OK();
  }

  RWLockWriteGuard guard(&rw_lock_);

  if (is_destroyed_) {
    std::string err_msg = fmt::format("[document_index.raw][id({})] document index is destroyed", id_);
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
    std::vector<std::string> date_column_names;
    std::vector<std::string> date_column_docs;
    std::vector<std::string> bool_column_names;
    std::vector<std::string> bool_column_docs;

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
        case pb::common::ScalarFieldType::DATETIME:
          date_column_names.push_back(field_name);
          date_column_docs.push_back(document_value.field_value().string_data());
          break;
        case pb::common::ScalarFieldType::BOOL:
          bool_column_names.push_back(field_name);
          if (document_value.field_value().bool_data()) {
            bool_column_docs.push_back("true");
          } else {
            bool_column_docs.push_back("false");
          }
          break;
        default:
          std::string err_msg =
              fmt::format("[document_index.raw][id({})] document_id: ({}) unknown field type({})", id_, document_id,
                          pb::common::ScalarFieldType_Name(document_value.field_type()));
          DINGO_LOG(ERROR) << err_msg;
          return butil::Status(pb::error::EILLEGAL_PARAMTETERS, err_msg);
          break;
      }
    }

    auto bool_result = ffi_index_multi_type_column_docs(
        index_path_, document_id, text_column_names, text_column_docs, i64_column_names, i64_column_docs,
        f64_column_names, f64_column_docs, bytes_column_names, bytes_column_docs, date_column_names, date_column_docs,
        bool_column_names, bool_column_docs);
    if (!bool_result.result) {
      std::string err_msg =
          fmt::format("[document_index.raw][id({})] document_id: ({}) add failed, error: {}, error_msg: {}", id_,
                      document_id, bool_result.error_code, bool_result.error_msg.c_str());
      DINGO_LOG(ERROR) << err_msg;
      return butil::Status(pb::error::EINTERNAL, err_msg);
    }
  }

  auto bool_result = ffi_index_writer_commit(index_path_);
  if (!bool_result.result) {
    std::string err_msg = fmt::format("[document_index.raw][id({})] commit failed, error: {}, error_msg: {}", id_,
                                      bool_result.error_code, bool_result.error_msg.c_str());
    DINGO_LOG(ERROR) << err_msg;
    return butil::Status(pb::error::EINTERNAL, err_msg);
  }

  if (reload_reader) {
    bool_result = ffi_index_reader_reload(index_path_);
    if (!bool_result.result) {
      std::string err_msg = fmt::format("[document_index.raw][id({})] reload failed, error: {}, error_msg: {}", id_,
                                        bool_result.error_code, bool_result.error_msg.c_str());
      DINGO_LOG(ERROR) << err_msg;
      return butil::Status(pb::error::EINTERNAL, err_msg);
    }
  }

  return butil::Status::OK();
}

butil::Status DocumentIndex::Delete(const std::vector<int64_t>& delete_ids) {
  DINGO_LOG(INFO) << fmt::format("[document_index.raw][id({})] delete document count({})", id_, delete_ids.size());

  if (delete_ids.empty()) {
    return butil::Status::OK();
  }

  RWLockWriteGuard guard(&rw_lock_);

  if (is_destroyed_) {
    std::string err_msg = fmt::format("[document_index.raw][id({})] document index is destroyed", id_);
    DINGO_LOG(ERROR) << err_msg;
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, err_msg);
  }

  std::vector<uint64_t> delete_ids_uint64;

  for (const auto& delete_id : delete_ids) {
    if (delete_id < 0) {
      std::string err_msg =
          fmt::format("[document_index.raw][id({})] delete failed, error: delete_id({}) < 0", id_, delete_id);
      DINGO_LOG(ERROR) << err_msg;
      return butil::Status(pb::error::EILLEGAL_PARAMTETERS, err_msg);
    } else if (delete_id >= INT64_MAX) {
      std::string err_msg =
          fmt::format("[document_index.raw][id({})] delete failed, error: delete_id({}) >= INT64_MAX", id_, delete_id);
      DINGO_LOG(ERROR) << err_msg;
      return butil::Status(pb::error::EILLEGAL_PARAMTETERS, err_msg);
    }

    delete_ids_uint64.push_back(delete_id);
  }

  auto bool_result = ffi_delete_row_ids(index_path_, delete_ids_uint64);
  if (!bool_result.result) {
    std::string err_msg = fmt::format("[document_index.raw][id({})] delete failed, error: {}, error_msg: {}", id_,
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
    std::string err_msg = fmt::format("[document_index.raw][id({})] document index is destroyed", id_);
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

  auto search_result = ffi_bm25_search_with_column_names(index_path_, query_string, topk, alive_ids, use_id_filter,
                                                         use_range_filter, start_id, end_id, column_names);

  if (search_result.error_code == 0) {
    for (const auto& row_id_with_score : search_result.result) {
      pb::common::DocumentWithScore result_doc;
      result_doc.mutable_document_with_id()->set_id(row_id_with_score.row_id);
      result_doc.set_score(row_id_with_score.score);
      results.push_back(result_doc);

      DINGO_LOG(INFO) << fmt::format("[document_index.raw][id({})] search result, row_id({}) score({})", id_,
                                     row_id_with_score.row_id, row_id_with_score.score);
    }

    return butil::Status::OK();
  } else {
    std::string err_msg = fmt::format("[document_index.raw][id({})] search failed, error: {}, error_msg: {}", id_,
                                      search_result.error_code, search_result.error_msg.c_str());
    DINGO_LOG(ERROR) << err_msg;
    return butil::Status(pb::error::EINTERNAL, err_msg);
  }
}

butil::Status DocumentIndex::Save(const std::string& /*path*/) {
  // Save need the caller to do LockWrite() and UnlockWrite()
  auto result = ffi_index_writer_commit(index_path_);
  if (result.result) {
    return butil::Status::OK();
  } else {
    std::string err_msg = fmt::format("[document_index.raw][id({})] save failed, error: {}, error_msg: {}", id_,
                                      result.error_code, result.error_msg.c_str());
    DINGO_LOG(ERROR) << err_msg;
    return butil::Status(pb::error::EINTERNAL, err_msg);
  }
}

butil::Status DocumentIndex::Load(const std::string& /*path*/) {
  auto result = ffi_index_reader_reload(index_path_);
  if (result.result) {
    return butil::Status::OK();
  } else {
    std::string err_msg = fmt::format("[document_index.raw][id({})] load failed, error: {}, error_msg: {}", id_,
                                      result.error_code, result.error_msg.c_str());
    DINGO_LOG(ERROR) << err_msg;
    return butil::Status(pb::error::EINTERNAL, err_msg);
  }
}

butil::Status DocumentIndex::GetDocCount(int64_t& count) {
  RWLockReadGuard guard(&rw_lock_);
  if (is_destroyed_) {
    std::string err_msg = fmt::format("[document_index.raw][id({})] document index is destroyed", id_);
    DINGO_LOG(ERROR) << err_msg;
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, err_msg);
  }

  count = ffi_get_indexed_doc_counts(index_path_);
  return butil::Status::OK();
}

butil::Status DocumentIndex::GetTokenCount(int64_t& count) {
  RWLockReadGuard guard(&rw_lock_);
  if (is_destroyed_) {
    std::string err_msg = fmt::format("[document_index.raw][id({})] document index is destroyed", id_);
    DINGO_LOG(ERROR) << err_msg;
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, err_msg);
  }

  count = ffi_get_total_num_tokens(index_path_);
  return butil::Status::OK();
}

butil::Status DocumentIndex::GetMetaJson(std::string& json) {
  RWLockReadGuard guard(&rw_lock_);
  if (is_destroyed_) {
    std::string err_msg = fmt::format("[document_index.raw][id({})] document index is destroyed", id_);
    DINGO_LOG(ERROR) << err_msg;
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, err_msg);
  }

  auto string_result = ffi_get_index_meta_json(index_path_);
  if (string_result.error_code != 0) {
    std::string err_msg = fmt::format("ffi_get_index_meta_json faild for ({}), error_code: ({}), error_msg: ({})",
                                      index_path_, string_result.error_code, string_result.error_msg.c_str());
    DINGO_LOG(ERROR) << err_msg;
    return butil::Status(pb::error::EINTERNAL, err_msg);
  }

  json = std::string(string_result.result.c_str());

  return butil::Status::OK();
}

butil::Status DocumentIndex::GetJsonParameter(std::string& json) {
  RWLockReadGuard guard(&rw_lock_);
  if (is_destroyed_) {
    std::string err_msg = fmt::format("[document_index.raw][id({})] document index is destroyed", id_);
    DINGO_LOG(ERROR) << err_msg;
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, err_msg);
  }

  auto string_result = ffi_get_index_json_parameter(index_path_);
  if (string_result.error_code != 0) {
    std::string err_msg = fmt::format("ffi_get_index_json_parameter faild for ({}), error_code: ({}), error_msg: ({})",
                                      index_path_, string_result.error_code, string_result.error_msg.c_str());
    DINGO_LOG(ERROR) << err_msg;
    return butil::Status(pb::error::EINTERNAL, err_msg);
  }

  json = std::string(string_result.result.c_str());

  return butil::Status::OK();
}

DocumentIndexWrapper::DocumentIndexWrapper(int64_t id, pb::common::DocumentIndexParameter index_parameter)
    : id_(id),
      ready_(false),
      destroyed_(false),
      is_switching_document_index_(false),
      index_parameter_(index_parameter),
      pending_task_num_(0),
      loadorbuilding_num_(0),
      rebuilding_num_(0) {
  bthread_mutex_init(&document_index_mutex_, nullptr);
  DINGO_LOG(DEBUG) << fmt::format("[new.DocumentIndexWrapper][id({})]", id_);
}

DocumentIndexWrapper::~DocumentIndexWrapper() {
  ClearDocumentIndex("destruct");

  bthread_mutex_destroy(&document_index_mutex_);
  DINGO_LOG(DEBUG) << fmt::format("[delete.DocumentIndexWrapper][id({})]", id_);

  if (IsDestoryed()) {
    RemoveMeta();
  }
}

std::shared_ptr<DocumentIndexWrapper> DocumentIndexWrapper::New(int64_t id,
                                                                pb::common::DocumentIndexParameter index_parameter) {
  auto document_index_wrapper = std::make_shared<DocumentIndexWrapper>(id, index_parameter);
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
  DINGO_LOG(INFO) << fmt::format("[document_index.wrapper][id({})] document index destroy.", Id());
  destroyed_.store(true);

  // destroy document index
  auto own_index = GetOwnDocumentIndex();
  if (own_index != nullptr) {
    own_index->SetDestroyed();
  }
}

bool DocumentIndexWrapper::Recover() {
  auto status = LoadMeta();
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("[document_index.wrapper][id({})] document index recover failed, error: {}", Id(),
                                    status.error_str());
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
    DINGO_LOG(ERROR) << fmt::format("[document_index.wrapper][id({})] delete document index meta failed.", Id());
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
  } else {
    DINGO_LOG(WARNING) << fmt::format("[document_index.wrapper][id({})] prase document index meta failed.", Id());
  }

  return butil::Status();
}

int64_t DocumentIndexWrapper::LastBuildEpochVersion() {
  auto document_index = GetOwnDocumentIndex();
  return (document_index != nullptr) ? document_index->Epoch().version() : 0;
}

int64_t DocumentIndexWrapper::ApplyLogId() const { return apply_log_id_.load(std::memory_order_acquire); }

void DocumentIndexWrapper::SetApplyLogId(int64_t apply_log_id) {
  // update inner document index apply log id
  if (apply_log_id - last_save_apply_log_id_.load(std::memory_order_relaxed) > FLAGS_document_index_save_log_gap) {
    last_save_apply_log_id_.store(apply_log_id, std::memory_order_relaxed);

    auto document_index = GetOwnDocumentIndex();
    if (document_index != nullptr) {
      auto status = document_index->SaveMeta(apply_log_id);
      if (!status.ok()) {
        DINGO_LOG(ERROR) << fmt::format("[document_index.raw][id({})] save meta fail.", id_);
      }
    }
  }

  apply_log_id_.store(apply_log_id, std::memory_order_release);
}

bool DocumentIndexWrapper::IsSwitchingDocumentIndex() { return is_switching_document_index_.load(); }

void DocumentIndexWrapper::SetIsSwitchingDocumentIndex(bool is_switching) {
  is_switching_document_index_.store(is_switching);
}

void DocumentIndexWrapper::UpdateDocumentIndex(DocumentIndexPtr document_index, const std::string& trace) {
  DINGO_LOG(INFO) << fmt::format(
      "[document_index.wrapper][id({})][trace({})] update document index, epoch({}) range({})", Id(), trace,
      Helper::RegionEpochToString(document_index->Epoch()), document_index->RangeString());

  // Check document index is stop
  if (IsDestoryed()) {
    DINGO_LOG(WARNING) << fmt::format("[document_index.wrapper][id({})] document index is stop.", Id());
    return;
  }

  {
    BAIDU_SCOPED_LOCK(document_index_mutex_);

    auto old_document_index = document_index_;

    // for document index, after update, the old index should be destroyed explicitly
    if (old_document_index != nullptr) {
      old_document_index->SetDestroyed();
    }

    document_index_ = document_index;

    share_document_index_ = nullptr;

    if (sibling_document_index_ != nullptr &&
        Helper::IsContainRange(document_index->Range(false), sibling_document_index_->Range(false))) {
      sibling_document_index_ = nullptr;
    }

    ++version_;

    ready_.store(true);

    DINGO_LOG(INFO) << fmt::format(
        "[document_index.wrapper][id({})][trace({})] update document index, apply_log_id({}/{}).", Id(), trace,
        apply_log_id_.load(std::memory_order_acquire), document_index->ApplyLogId());

    apply_log_id_.store(document_index->ApplyLogId(), std::memory_order_release);

    SaveMeta();
  }
}

void DocumentIndexWrapper::ClearDocumentIndex(const std::string& trace) {
  DINGO_LOG(INFO) << fmt::format("[document_index.wrapper][id({})][trace({})] Clear all document index", Id(), trace);

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

// Filter document id by range
static std::vector<int64_t> FilterDocumentId(const std::vector<pb::common::DocumentWithId>& document_with_ids,
                                             const pb::common::Range& range) {
  int64_t begin_document_id = 0, end_document_id = 0;
  DocumentCodec::DecodeRangeToDocumentId(false, range, begin_document_id, end_document_id);

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
  DocumentCodec::DecodeRangeToDocumentId(false, range, begin_document_id, end_document_id);

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
  DocumentCodec::DecodeRangeToDocumentId(false, range, begin_document_id, end_document_id);

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
    DINGO_LOG(WARNING) << fmt::format("[document_index.wrapper][id({})] document index is not ready.", Id());
    return butil::Status(pb::error::EDOCUMENT_INDEX_NOT_FOUND, "document index %lu is not ready.", Id());
  }

  // Waiting switch document index
  int count = 0;
  while (IsSwitchingDocumentIndex()) {
    DINGO_LOG(INFO) << fmt::format("[document_index.wrapper][id({})] waiting document index switch, count({})", Id(),
                                   ++count);
    bthread_usleep(1000 * 100);
  }

  auto document_index = GetDocumentIndex();
  if (document_index == nullptr) {
    DINGO_LOG(WARNING) << fmt::format("[document_index.wrapper][id({})] document index is not ready.", Id());
    return butil::Status(pb::error::EDOCUMENT_INDEX_NOT_FOUND, "document index %lu is not ready.", Id());
  }

  // Exist sibling document index, so need to separate add document.
  auto sibling_document_index = SiblingDocumentIndex();
  if (sibling_document_index != nullptr) {
    auto status = sibling_document_index->Upsert(
        FilterDocumentWithId(document_with_ids, sibling_document_index->Range(false)), true);
    if (!status.ok()) {
      return status;
    }

    status = document_index->Upsert(FilterDocumentWithId(document_with_ids, document_index->Range(false)), true);
    if (!status.ok()) {
      sibling_document_index->Delete(FilterDocumentId(document_with_ids, sibling_document_index->Range(false)));
      return status;
    }

    return status;
  }

  return document_index->Upsert(document_with_ids, true);
}

butil::Status DocumentIndexWrapper::Add(const std::vector<pb::common::DocumentWithId>& document_with_ids) {
  if (!IsReady()) {
    DINGO_LOG(WARNING) << fmt::format("[document_index.wrapper][id({})] document index is not ready.", Id());
    return butil::Status(pb::error::EDOCUMENT_INDEX_NOT_FOUND, "document index %lu is not ready.", Id());
  }

  // Waiting switch document index
  int count = 0;
  while (IsSwitchingDocumentIndex()) {
    DINGO_LOG(INFO) << fmt::format("[document_index.wrapper][id({})] waiting document index switch, count({})", Id(),
                                   ++count);
    bthread_usleep(1000 * 100);
  }

  auto document_index = GetDocumentIndex();
  if (document_index == nullptr) {
    DINGO_LOG(WARNING) << fmt::format("[document_index.wrapper][id({})] document index is not ready.", Id());
    return butil::Status(pb::error::EDOCUMENT_INDEX_NOT_FOUND, "document index %lu is not ready.", Id());
  }

  // Exist sibling document index, so need to separate add document.
  auto sibling_document_index = SiblingDocumentIndex();
  if (sibling_document_index != nullptr) {
    auto status = sibling_document_index->Add(
        FilterDocumentWithId(document_with_ids, sibling_document_index->Range(false)), true);
    if (!status.ok()) {
      return status;
    }

    status = document_index->Add(FilterDocumentWithId(document_with_ids, document_index->Range(false)), true);
    if (!status.ok()) {
      sibling_document_index->Delete(FilterDocumentId(document_with_ids, sibling_document_index->Range(false)));
      return status;
    }

    return status;
  }

  return document_index->Add(document_with_ids, true);
}

butil::Status DocumentIndexWrapper::Delete(const std::vector<int64_t>& delete_ids) {
  if (!IsReady()) {
    DINGO_LOG(WARNING) << fmt::format("[document_index.wrapper][id({})] document index is not ready.", Id());
    return butil::Status(pb::error::EDOCUMENT_INDEX_NOT_FOUND, "document index %lu is not ready.", Id());
  }

  // Switch document index wait
  int count = 0;
  while (IsSwitchingDocumentIndex()) {
    DINGO_LOG(INFO) << fmt::format("[document_index.wrapper][id({})] document index switch waiting, count({})", Id(),
                                   ++count);
    bthread_usleep(1000 * 100);
  }

  auto document_index = GetDocumentIndex();
  if (document_index == nullptr) {
    DINGO_LOG(WARNING) << fmt::format("[document_index.wrapper][id({})] document index is not ready.", Id());
    return butil::Status(pb::error::EDOCUMENT_INDEX_NOT_FOUND, "document index %lu is not ready.", Id());
  }

  // Exist sibling document index, so need to separate delete document.
  auto sibling_document_index = SiblingDocumentIndex();
  if (sibling_document_index != nullptr) {
    auto status = sibling_document_index->Delete(FilterDocumentId(delete_ids, sibling_document_index->Range(false)));
    if (!status.ok()) {
      return status;
    }

    return document_index->Delete(FilterDocumentId(delete_ids, document_index->Range(false)));
  }

  return document_index->Delete(delete_ids);
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
    DINGO_LOG(WARNING) << fmt::format("[document_index.wrapper][id({})] document index is not ready.", Id());
    return butil::Status(pb::error::EDOCUMENT_INDEX_NOT_FOUND, "document index %lu is not ready.", Id());
  }
  auto document_index = GetDocumentIndex();
  if (document_index == nullptr) {
    DINGO_LOG(WARNING) << fmt::format("[document_index.wrapper][id({})] document index is not ready.", Id());
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
    DINGO_LOG(INFO) << fmt::format("[document_index.wrapper][id({})] search document in sibling document index.", Id());
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

  const auto& index_range = document_index->Range(false);
  if (region_range.start_key() != index_range.start_key() || region_range.end_key() != index_range.end_key()) {
    int64_t min_document_id = 0, max_document_id = 0;
    DocumentCodec::DecodeRangeToDocumentId(false, region_range, min_document_id, max_document_id);

    DINGO_LOG(INFO) << fmt::format(
        "[document_index.wrapper][id({})] search document in document index with range_filter, range({}) "
        "query_string({}) top_n({}) min_document_id({}) max_document_id({})",
        Id(), DocumentCodec::DebugRange(false, region_range), parameter.query_string(), parameter.top_n(),
        min_document_id, max_document_id);

    // use range filter
    return document_index->Search(parameter.top_n(), parameter.query_string(), true, min_document_id, max_document_id,
                                  use_id_filter, alive_ids, column_names, results);
  }

  DINGO_LOG(INFO) << fmt::format(
      "[document_index.wrapper][id({})] search document in document index, range({}) query_string({}) top_n({})", Id(),
      DocumentCodec::DebugRange(false, region_range), parameter.query_string(), parameter.top_n());

  return document_index->Search(parameter.top_n(), parameter.query_string(), false, 0, INT64_MAX, use_id_filter,
                                alive_ids, column_names, results);
}

// For document index, all node need to hold the index, so this function always return true.
bool DocumentIndexWrapper::IsPermanentHoldDocumentIndex(int64_t /*region_id*/) { return true; }

}  // namespace dingodb
