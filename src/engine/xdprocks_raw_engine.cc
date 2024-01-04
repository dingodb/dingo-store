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

#include "engine/xdprocks_raw_engine.h"

#include <elf.h>

#include <algorithm>
#include <climits>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <exception>
#include <filesystem>
#include <iomanip>
#include <map>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>

#include "butil/compiler_specific.h"
#include "butil/status.h"
#include "common/constant.h"
#include "common/helper.h"
#include "common/logging.h"
#include "config/config_helper.h"
#include "engine/raw_engine.h"
#include "engine/snapshot.h"
#include "fmt/core.h"
#include "proto/common.pb.h"
#include "proto/error.pb.h"
#include "xdprocks/advanced_options.h"
#include "xdprocks/cache.h"
#include "xdprocks/db.h"
#include "xdprocks/filter_policy.h"
#include "xdprocks/iterator.h"
#include "xdprocks/table.h"
#include "xdprocks/write_batch.h"

namespace dingodb {

namespace xdp {

ColumnFamily::ColumnFamily(const std::string& cf_name, const ColumnFamilyConfig& config,
                           xdprocks::ColumnFamilyHandle* handle)
    : name_(cf_name), config_(config), handle_(handle) {}

ColumnFamily::ColumnFamily(const std::string& cf_name, const ColumnFamilyConfig& config)
    : ColumnFamily(cf_name, config, nullptr) {}

ColumnFamily::~ColumnFamily() {
  delete handle_;
  handle_ = nullptr;
}

void ColumnFamily::SetConfItem(const std::string& name, const std::string& value) {
  auto it = config_.find(name);
  if (it == config_.end()) {
    config_.insert(std::make_pair(name, value));
  } else {
    it->second = value;
  }
}

std::string ColumnFamily::GetConfItem(const std::string& name) {
  auto it = config_.find(name);
  return it == config_.end() ? "" : it->second;
}

void ColumnFamily::Dump() {
  for (const auto& [name, value] : config_) {
    DINGO_LOG(INFO) << fmt::format("[xdprocks.dump][column_family({})] {} : {}", Name(), name, value);
  }

  DINGO_LOG(INFO) << fmt::format("[xdprocks.dump][column_family({})] end.....................", Name());
}

bool Iterator::Valid() const {
  if (!iter_->Valid()) {
    return false;
  }

  if (!options_.upper_bound.empty()) {
    auto upper_bound = xdprocks::Slice(options_.upper_bound);
    if (upper_bound.compare(iter_->key()) <= 0) {
      return false;
    }
  }
  if (!options_.lower_bound.empty()) {
    auto lower_bound = xdprocks::Slice(options_.lower_bound);
    if (lower_bound.compare(iter_->key()) > 0) {
      return false;
    }
  }

  return true;
}

butil::Status Iterator::Status() const {
  if (iter_->status().ok()) {
    return butil::Status();
  }
  return butil::Status(pb::error::EINTERNAL, "Internal iterator error");
}

butil::Status SstFileWriter::SaveFile(const std::vector<pb::common::KeyValue>& kvs, const std::string& filename) {
  auto status = sst_writer_->Open(filename);
  if (!status.ok()) {
    return butil::Status(status.code(), status.ToString());
  }

  for (const auto& kv : kvs) {
    status = sst_writer_->Put(kv.key(), kv.value());
    if (!status.ok()) {
      return butil::Status(status.code(), status.ToString());
    }
  }

  status = sst_writer_->Finish();
  if (!status.ok()) {
    return butil::Status(status.code(), status.ToString());
  }

  return butil::Status();
}

butil::Status SstFileWriter::SaveFile(std::shared_ptr<dingodb::Iterator> iter, const std::string& filename) {
  auto status = sst_writer_->Open(filename);
  if (!status.ok()) {
    return butil::Status(status.code(), status.ToString());
  }

  for (; iter->Valid(); iter->Next()) {
    status = sst_writer_->Put(iter->Key(), iter->Value());
    if (!status.ok()) {
      sst_writer_->Finish();
      return butil::Status(status.code(), status.ToString());
    }
  }

  status = sst_writer_->Finish();
  if (!status.ok()) {
    return butil::Status((status.code() == xdprocks::Status::Code::kInvalidArgument &&
                          status.ToString().find("no entries") != std::string::npos)
                             ? pb::error::ENO_ENTRIES
                             : static_cast<int>(status.code()),
                         status.ToString());
  }

  return butil::Status();
}

std::shared_ptr<XDPRocksRawEngine> Checkpoint::GetRawEngine() {
  auto raw_engine = raw_engine_.lock();
  if (raw_engine == nullptr) {
    DINGO_LOG(FATAL) << "[xdprocks] get raw engine failed.";
  }

  return raw_engine;
}

std::shared_ptr<xdprocks::DB> Checkpoint::GetDB() { return GetRawEngine()->GetDB(); }
std::vector<xdp::ColumnFamilyPtr> Checkpoint::GetColumnFamilies(const std::vector<std::string>& cf_names) {
  return GetRawEngine()->GetColumnFamilies(cf_names);
}

butil::Status Checkpoint::Create(const std::string& dirpath) {
  // std::unique_ptr<xdprocks::Checkpoint> checkpoint = std::make_unique<xdprocks::Checkpoint>();
  xdprocks::Checkpoint* checkpoint = nullptr;
  auto status = xdprocks::Checkpoint::Create(GetDB().get(), &checkpoint);
  if (!status.ok()) {
    delete checkpoint;
    return butil::Status(status.code(), status.ToString());
  }

  status = checkpoint->CreateCheckpoint(dirpath);
  if (!status.ok()) {
    delete checkpoint;
    return butil::Status(status.code(), status.ToString());
  }

  delete checkpoint;
  return butil::Status();
}

butil::Status Checkpoint::Create(const std::string& dirpath, const std::vector<std::string>& cf_names,
                                 std::vector<pb::store_internal::SstFileInfo>& sst_files) {
  xdprocks::Checkpoint* checkpoint = nullptr;
  auto status = xdprocks::Checkpoint::Create(GetDB().get(), &checkpoint);
  if (!status.ok()) {
    delete checkpoint;
    DINGO_LOG(ERROR) << fmt::format("[xdprocks] create checkpoint failed, error: {}.", status.ToString());
    return butil::Status(status.code(), status.ToString());
  }

  status = GetDB()->DisableFileDeletions();
  if (!status.ok()) {
    delete checkpoint;
    DINGO_LOG(ERROR) << fmt::format("[xdprocks] disable file deletion failed, error: {}.", status.ToString());
    return butil::Status(status.code(), status.ToString());
  }

  status = checkpoint->CreateCheckpoint(dirpath);
  if (!status.ok()) {
    GetDB()->EnableFileDeletions(false);
    DINGO_LOG(ERROR) << fmt::format("[xdprocks] export column family checkpoint failed, error: {}.", status.ToString());
    delete checkpoint;
    return butil::Status(status.code(), status.ToString());
  }

  std::vector<xdprocks::ColumnFamilyMetaData> meta_datas;

  auto column_families = GetColumnFamilies(cf_names);
  for (const auto& column_family : column_families) {
    xdprocks::ColumnFamilyMetaData meta_data;
    GetDB()->GetColumnFamilyMetaData(column_family->GetHandle(), &meta_data);
    meta_datas.push_back(meta_data);
  }

  status = GetDB()->EnableFileDeletions(false);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("[xdprocks] enable file deletion failed, error: {}.", status.ToString());
    return butil::Status(status.code(), status.ToString());
  }

  if (column_families.size() != meta_datas.size()) {
    DINGO_LOG(ERROR) << fmt::format("[xdprocks] column_families.size() != meta_datas.size()") << column_families.size()
                     << " != " << meta_datas.size();
    return butil::Status(pb::error::EINTERNAL, "Internal error");
  }

  for (int i = 0; i < column_families.size(); i++) {
    auto& meta_data = meta_datas[i];
    auto& column_family = column_families[i];

    for (auto& level : meta_data.levels) {
      for (const auto& file : level.files) {
        std::string filepath = dirpath + file.name;
        if (!Helper::IsExistPath(filepath)) {
          DINGO_LOG(INFO) << fmt::format("[xdprocks] checkpoint not contain sst file: {}", filepath);
          continue;
        }

        pb::store_internal::SstFileInfo sst_file;
        sst_file.set_level(level.level);
        sst_file.set_name(file.name);
        sst_file.set_path(filepath);
        sst_file.set_start_key(file.smallestkey);
        sst_file.set_end_key(file.largestkey);
        sst_file.set_cf_name(column_family->Name());

        DINGO_LOG(DEBUG) << "checkpoint add sst_file: " << sst_file.ShortDebugString();

        sst_files.emplace_back(std::move(sst_file));
      }
    }
  }

  pb::store_internal::SstFileInfo sst_file;
  sst_file.set_level(-1);
  sst_file.set_name("CURRENT");
  sst_file.set_path(dirpath + "/CURRENT");
  sst_files.push_back(sst_file);

  std::string manifest_name = Helper::FindFileInDirectory(dirpath, "MANIFEST");
  sst_file.set_level(-1);
  sst_file.set_name(manifest_name);
  sst_file.set_path(dirpath + "/" + manifest_name);
  sst_files.push_back(sst_file);

  std::string options_name = Helper::FindFileInDirectory(dirpath, "OPTIONS");
  sst_file.set_level(-1);
  sst_file.set_name(options_name);
  sst_file.set_path(dirpath + "/" + options_name);
  sst_files.push_back(sst_file);

  delete checkpoint;
  return butil::Status();
}

std::shared_ptr<XDPRocksRawEngine> Reader::GetRawEngine() {
  auto raw_engine = raw_engine_.lock();
  if (raw_engine == nullptr) {
    DINGO_LOG(FATAL) << "[xdprocks] get raw engine failed.";
  }

  return raw_engine;
}

dingodb::SnapshotPtr Reader::GetSnapshot() { return GetRawEngine()->GetSnapshot(); }

std::shared_ptr<xdprocks::DB> Reader::GetDB() { return GetRawEngine()->GetDB(); }

ColumnFamilyPtr Reader::GetColumnFamily(const std::string& cf_name) { return GetRawEngine()->GetColumnFamily(cf_name); }

ColumnFamilyPtr Reader::GetDefaultColumnFamily() { return GetRawEngine()->GetDefaultColumnFamily(); }

butil::Status Reader::KvGet(const std::string& cf_name, const std::string& key, std::string& value) {
  return KvGet(GetColumnFamily(cf_name), GetSnapshot(), key, value);
}

butil::Status Reader::KvGet(const std::string& cf_name, std::shared_ptr<dingodb::Snapshot> snapshot,
                            const std::string& key, std::string& value) {
  auto column_family = GetColumnFamily(cf_name);
  return KvGet(column_family, snapshot, key, value);
}

butil::Status Reader::KvGet(ColumnFamilyPtr column_family, dingodb::SnapshotPtr snapshot, const std::string& key,
                            std::string& value) {
  if (BAIDU_UNLIKELY(key.empty())) {
    DINGO_LOG(ERROR) << fmt::format("[xdprocks] not support empty key.");
    return butil::Status(pb::error::EKEY_EMPTY, "Key is empty");
  }

  xdprocks::ReadOptions read_option;
  read_option.snapshot = static_cast<const xdprocks::Snapshot*>(snapshot->Inner());
  xdprocks::Status s = GetDB()->Get(read_option, column_family->GetHandle(), xdprocks::Slice(key), &value);
  if (!s.ok()) {
    if (s.IsNotFound()) {
      return butil::Status(pb::error::EKEY_NOT_FOUND, "Not found key");
    }
    DINGO_LOG(ERROR) << fmt::format("[xdprocks] get key failed, error: {}", s.ToString());
    return butil::Status(pb::error::EINTERNAL, "Internal get error");
  }

  return butil::Status();
}

butil::Status Reader::KvScan(ColumnFamilyPtr column_family, std::shared_ptr<dingodb::Snapshot> snapshot,
                             const std::string& start_key, const std::string& end_key,
                             std::vector<pb::common::KeyValue>& kvs) {
  if (BAIDU_UNLIKELY(start_key.empty())) {
    DINGO_LOG(ERROR) << fmt::format("[xdprocks] not support empty start_key.");
    return butil::Status(pb::error::EKEY_EMPTY, "Key is empty");
  }

  if (BAIDU_UNLIKELY(end_key.empty())) {
    DINGO_LOG(ERROR) << fmt::format("[xdprocks] not support empty end_key.");
    return butil::Status(pb::error::EKEY_EMPTY, "Key is empty");
  }

  xdprocks::ReadOptions read_option;
  read_option.auto_prefix_mode = true;
  read_option.snapshot = static_cast<const xdprocks::Snapshot*>(snapshot->Inner());

  std::string_view end_key_view(end_key);
  xdprocks::Iterator* it = GetDB()->NewIterator(read_option, column_family->GetHandle());
  for (it->Seek(start_key); it->Valid() && it->key().ToStringView() < end_key_view; it->Next()) {
    pb::common::KeyValue kv;
    kv.set_key(it->key().data(), it->key().size());
    kv.set_value(it->value().data(), it->value().size());

    kvs.emplace_back(std::move(kv));
  }
  delete it;

  return butil::Status();
}

butil::Status Reader::KvScan(const std::string& cf_name, const std::string& start_key, const std::string& end_key,
                             std::vector<pb::common::KeyValue>& kvs) {
  return KvScan(GetColumnFamily(cf_name), GetSnapshot(), start_key, end_key, kvs);
}

butil::Status Reader::KvScan(const std::string& cf_name, std::shared_ptr<dingodb::Snapshot> snapshot,
                             const std::string& start_key, const std::string& end_key,
                             std::vector<pb::common::KeyValue>& kvs) {
  return KvScan(GetColumnFamily(cf_name), snapshot, start_key, end_key, kvs);
}

butil::Status Reader::KvCount(ColumnFamilyPtr column_family, dingodb::SnapshotPtr snapshot,
                              const std::string& start_key, const std::string& end_key, int64_t& count) {
  if (BAIDU_UNLIKELY(start_key.empty())) {
    DINGO_LOG(ERROR) << fmt::format("[xdprocks] not support empty start_key.");
    return butil::Status(pb::error::EKEY_EMPTY, "Key is empty");
  }

  if (BAIDU_UNLIKELY(end_key.empty())) {
    DINGO_LOG(ERROR) << fmt::format("[xdprocks] not support empty end_key.");
    return butil::Status(pb::error::EKEY_EMPTY, "Key is empty");
  }

  xdprocks::ReadOptions read_options;
  read_options.auto_prefix_mode = true;
  read_options.snapshot = static_cast<const xdprocks::Snapshot*>(snapshot->Inner());

  std::string_view end_key_view(end_key.data(), end_key.size());
  xdprocks::Iterator* it = GetDB()->NewIterator(read_options, column_family->GetHandle());
  for (it->Seek(start_key), count = 0; it->Valid() && it->key().ToStringView() < end_key_view; it->Next()) {
    ++count;
  }
  delete it;

  return butil::Status();
}

butil::Status Reader::KvCount(const std::string& cf_name, const std::string& start_key, const std::string& end_key,
                              int64_t& count) {
  return KvCount(GetColumnFamily(cf_name), GetSnapshot(), start_key, end_key, count);
}

butil::Status Reader::KvCount(const std::string& cf_name, dingodb::SnapshotPtr snapshot, const std::string& start_key,
                              const std::string& end_key, int64_t& count) {
  return KvCount(GetColumnFamily(cf_name), snapshot, start_key, end_key, count);
}

dingodb::IteratorPtr Reader::NewIterator(ColumnFamilyPtr column_family, dingodb::SnapshotPtr snapshot,
                                         IteratorOptions options) {
  // Correct free iterate_upper_bound
  // auto slice = std::make_unique<xdprocks::Slice>(options.upper_bound);
  xdprocks::ReadOptions read_options;
  if (snapshot != nullptr) {
    read_options.snapshot = static_cast<const xdprocks::Snapshot*>(snapshot->Inner());
  }
  read_options.auto_prefix_mode = true;

  return std::make_shared<Iterator>(options, GetDB()->NewIterator(read_options, column_family->GetHandle()), snapshot);
}

dingodb::IteratorPtr Reader::NewIterator(const std::string& cf_name, IteratorOptions options) {
  return NewIterator(GetColumnFamily(cf_name), GetSnapshot(), options);
}

dingodb::IteratorPtr Reader::NewIterator(const std::string& cf_name, dingodb::SnapshotPtr snapshot,
                                         IteratorOptions options) {
  return NewIterator(GetColumnFamily(cf_name), snapshot, options);
}

std::shared_ptr<XDPRocksRawEngine> Writer::GetRawEngine() {
  auto raw_engine = raw_engine_.lock();
  if (raw_engine == nullptr) {
    DINGO_LOG(FATAL) << "[xdprocks] get raw engine failed.";
  }

  return raw_engine;
}

std::shared_ptr<xdprocks::DB> Writer::GetDB() { return GetRawEngine()->GetDB(); }

ColumnFamilyPtr Writer::GetColumnFamily(const std::string& cf_name) { return GetRawEngine()->GetColumnFamily(cf_name); }

ColumnFamilyPtr Writer::GetDefaultColumnFamily() { return GetRawEngine()->GetDefaultColumnFamily(); }

butil::Status Writer::KvPut(const std::string& cf_name, const pb::common::KeyValue& kv) {
  if (BAIDU_UNLIKELY(kv.key().empty())) {
    DINGO_LOG(ERROR) << fmt::format("[xdprocks] not support empty key.");
    return butil::Status(pb::error::EKEY_EMPTY, "Key is empty");
  }

  xdprocks::WriteOptions write_options;
  xdprocks::Status s = GetDB()->Put(write_options, GetColumnFamily(cf_name)->GetHandle(), xdprocks::Slice(kv.key()),
                                    xdprocks::Slice(kv.value()));
  if (!s.ok()) {
    DINGO_LOG(ERROR) << fmt::format("[xdprocks] put failed, error: {}.", s.ToString());
    return butil::Status(pb::error::EINTERNAL, "Internal put error");
  }

  return butil::Status();
}

butil::Status Writer::KvBatchPutAndDelete(const std::string& cf_name,
                                          const std::vector<pb::common::KeyValue>& kvs_to_put,
                                          const std::vector<std::string>& keys_to_delete) {
  if (BAIDU_UNLIKELY(kvs_to_put.empty() && keys_to_delete.empty())) {
    DINGO_LOG(ERROR) << fmt::format("[xdprocks] not support empty keys.");
    return butil::Status(pb::error::EKEY_EMPTY, "Key is empty");
  }

  auto column_family = GetColumnFamily(cf_name);

  xdprocks::WriteBatch batch;
  for (const auto& kv : kvs_to_put) {
    if (BAIDU_UNLIKELY(kv.key().empty())) {
      DINGO_LOG(ERROR) << fmt::format("[xdprocks] not support empty key.");
      return butil::Status(pb::error::EKEY_EMPTY, "Key is empty");
    } else {
      xdprocks::Status s = batch.Put(column_family->GetHandle(), kv.key(), kv.value());
      if (BAIDU_UNLIKELY(!s.ok())) {
        DINGO_LOG(ERROR) << fmt::format("[xdprocks] batch put failed, error: {}.", s.ToString());
        return butil::Status(pb::error::EINTERNAL, "Internal put error");
      }
    }
  }

  for (const auto& key : keys_to_delete) {
    if (BAIDU_UNLIKELY(key.empty())) {
      DINGO_LOG(ERROR) << fmt::format("[xdprocks] not support empty key.");
      return butil::Status(pb::error::EKEY_EMPTY, "Key is empty");
    } else {
      xdprocks::Status s = batch.Delete(column_family->GetHandle(), key);
      if (BAIDU_UNLIKELY(!s.ok())) {
        DINGO_LOG(ERROR) << fmt::format("[xdprocks] batch delete failed, error: {}.", s.ToString());
        return butil::Status(pb::error::EINTERNAL, "Internal delete error");
      }
    }
  }
  xdprocks::WriteOptions write_options;
  xdprocks::Status s = GetDB()->Write(write_options, &batch);
  if (!s.ok()) {
    DINGO_LOG(ERROR) << fmt::format("[xdprocks] write failed, error: {}", s.ToString());
    return butil::Status(pb::error::EINTERNAL, "Internal write error");
  }

  return butil::Status();
}

butil::Status Writer::KvBatchPutAndDelete(
    const std::map<std::string, std::vector<pb::common::KeyValue>>& kv_puts_with_cf,
    const std::map<std::string, std::vector<std::string>>& kv_deletes_with_cf) {
  DINGO_LOG(INFO) << fmt::format("[xdprocks] KvBatchPutAndDelete put kv size: {} delete kv size: {}",
                                 kv_puts_with_cf.size(), kv_deletes_with_cf.size());

  xdprocks::WriteBatch batch;
  for (const auto& [cf_name, kv_puts] : kv_puts_with_cf) {
    if (BAIDU_UNLIKELY(kv_puts.empty())) {
      DINGO_LOG(ERROR) << fmt::format("[xdprocks] keys empty not support");
      return butil::Status(pb::error::EKEY_EMPTY, "Key is empty");
    }

    auto column_family = GetColumnFamily(cf_name);
    for (const auto& kv : kv_puts) {
      if (BAIDU_UNLIKELY(kv.key().empty())) {
        DINGO_LOG(ERROR) << fmt::format("[xdprocks] key empty not support");
        return butil::Status(pb::error::EKEY_EMPTY, "Key is empty");
      }

      xdprocks::Status s = batch.Put(column_family->GetHandle(), kv.key(), kv.value());
      if (BAIDU_UNLIKELY(!s.ok())) {
        DINGO_LOG(ERROR) << fmt::format("[xdprocks] put failed, error: {}", s.ToString());
        return butil::Status(pb::error::EINTERNAL, "Internal put error");
      }
    }
  }

  for (const auto& [cf_name, kv_deletes] : kv_deletes_with_cf) {
    if (BAIDU_UNLIKELY(kv_deletes.empty())) {
      DINGO_LOG(ERROR) << fmt::format("[xdprocks] keys empty not support");
      return butil::Status(pb::error::EKEY_EMPTY, "Key is empty");
    }

    auto column_family = GetColumnFamily(cf_name);
    for (const auto& key : kv_deletes) {
      if (BAIDU_UNLIKELY(key.empty())) {
        DINGO_LOG(ERROR) << fmt::format("[xdprocks] key empty not support");
        return butil::Status(pb::error::EKEY_EMPTY, "Key is empty");
      }

      xdprocks::Status s = batch.Delete(column_family->GetHandle(), key);
      if (BAIDU_UNLIKELY(!s.ok())) {
        DINGO_LOG(ERROR) << fmt::format("[xdprocks] put failed, error: {}", s.ToString());
        return butil::Status(pb::error::EINTERNAL, "Internal delete error");
      }
    }
  }

  xdprocks::WriteOptions write_options;
  xdprocks::Status s = GetDB()->Write(write_options, &batch);
  if (!s.ok()) {
    DINGO_LOG(ERROR) << fmt::format("[xdprocks] write failed, error: {}", s.ToString());
    return butil::Status(pb::error::EINTERNAL, fmt::format("xdprocks::DB::Write failed : {}", s.ToString()));
  }

  return butil::Status::OK();
}

butil::Status Writer::KvDelete(const std::string& cf_name, const std::string& key) {
  if (BAIDU_UNLIKELY(key.empty())) {
    DINGO_LOG(ERROR) << fmt::format("[xdprocks] not support empty key.");
    return butil::Status(pb::error::EKEY_EMPTY, "Key is empty");
  }

  xdprocks::WriteOptions const write_options;
  xdprocks::Status const s =
      GetDB()->Delete(write_options, GetColumnFamily(cf_name)->GetHandle(), xdprocks::Slice(key.data(), key.size()));
  if (!s.ok()) {
    DINGO_LOG(ERROR) << fmt::format("[xdprocks] delete failed, error: {}.", s.ToString());
    return butil::Status(pb::error::EINTERNAL, "Internal delete error");
  }

  return butil::Status();
}

butil::Status Writer::KvDeleteRange(const std::string& cf_name, const pb::common::Range& range) {
  if (range.start_key().empty() || range.end_key().empty()) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "range is empty");
  }
  if (range.start_key() >= range.end_key()) {
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "range is wrong");
  }

  xdprocks::WriteBatch batch;
  xdprocks::Status s = batch.DeleteRange(GetColumnFamily(cf_name)->GetHandle(), range.start_key(), range.end_key());
  if (!s.ok()) {
    DINGO_LOG(ERROR) << fmt::format("[xdprocks] delete range failed, error: {}.", s.ToString());
    return butil::Status(pb::error::EINTERNAL, "Internal delete range error");
  }

  s = GetDB()->Write(xdprocks::WriteOptions(), &batch);
  if (!s.ok()) {
    DINGO_LOG(ERROR) << fmt::format("[xdprocks] write failed, error: {}.", s.ToString());
    return butil::Status(pb::error::EINTERNAL, "Internal write error");
  }

  return butil::Status();
}

butil::Status Writer::KvBatchDeleteRange(const std::map<std::string, std::vector<pb::common::Range>>& range_with_cfs) {
  xdprocks::WriteBatch batch;
  for (const auto& [cf_name, ranges] : range_with_cfs) {
    auto column_family = GetColumnFamily(cf_name);
    for (const auto& range : ranges) {
      if (range.start_key().empty() || range.end_key().empty()) {
        return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "range is empty");
      }
      if (range.start_key() >= range.end_key()) {
        return butil::Status(pb::error::EILLEGAL_PARAMTETERS, "range is wrong");
      }

      xdprocks::Status s = batch.DeleteRange(column_family->GetHandle(), range.start_key(), range.end_key());
      if (!s.ok()) {
        DINGO_LOG(ERROR) << fmt::format("[xdprocks] delete range failed, error: {}.", s.ToString());
        return butil::Status(pb::error::EINTERNAL, "Internal delete range error");
      }
    }
  }

  xdprocks::Status s = GetDB()->Write(xdprocks::WriteOptions(), &batch);
  if (!s.ok()) {
    DINGO_LOG(ERROR) << fmt::format("[xdprocks] write failed, error: {}.", s.ToString());
    return butil::Status(pb::error::EINTERNAL, "Internal write error");
  }

  return butil::Status();
}

}  // namespace xdp

XDPRocksRawEngine::XDPRocksRawEngine() : db_(nullptr), column_families_({}) {}

XDPRocksRawEngine::~XDPRocksRawEngine() = default;

static xdp::ColumnFamilyMap GenColumnFamilyByDefaultConfig(const std::vector<std::string>& column_family_names) {
  xdp::ColumnFamily::ColumnFamilyConfig default_config;
  default_config.emplace(Constant::kBlockSize, "4096");
  default_config.emplace(Constant::kBlockCache, Constant::kBlockCacheDefaultValue);
  default_config.emplace(Constant::kArenaBlockSize, Constant::kArenaBlockSizeDefaultValue);
  default_config.emplace(Constant::kMinWriteBufferNumberToMerge, Constant::kMinWriteBufferNumberToMergeDefaultValue);
  default_config.emplace(Constant::kMaxWriteBufferNumber, Constant::kMaxWriteBufferNumberDefaultValue);
  default_config.emplace(Constant::kMaxCompactionBytes, Constant::kMaxCompactionBytesDefaultValue);
  default_config.emplace(Constant::kWriteBufferSize, Constant::kWriteBufferSizeDefaultValue);
  default_config.emplace(Constant::kPrefixExtractor, Constant::kPrefixExtractorDefaultValue);
  default_config.emplace(Constant::kMaxBytesForLevelBase, Constant::kMaxBytesForLevelBaseDefaultValue);
  default_config.emplace(Constant::kTargetFileSizeBase, Constant::kTargetFileSizeBaseDefaultValue);
  default_config.emplace(Constant::kMaxBytesForLevelMultiplier, Constant::kMaxBytesForLevelMultiplierDefaultValue);

  xdp::ColumnFamilyMap column_families;
  for (const auto& cf_name : column_family_names) {
    column_families.emplace(cf_name, xdp::ColumnFamily::New(cf_name, default_config));
  }

  return column_families;
}

static void SetColumnFamilyCustomConfig(const std::shared_ptr<Config>& config, xdp::ColumnFamilyMap& column_families) {
  // store.base config
  const auto base_cf_config = config->GetStringMap(Constant::kBaseColumnFamily);
  auto base_column_family_names = config->GetStringList(Constant::kColumnFamilies);
  for (const auto& cf_name : base_column_family_names) {
    auto it = column_families.find(cf_name);
    if (it == column_families.end()) {
      continue;
    }
    auto& column_family = it->second;
    for (const auto& [name, value] : base_cf_config) {
      column_family->SetConfItem(name, value);
    }
  }

  // // store.[cf_name] config
  for (auto& [cf_name, column_family] : column_families) {
    std::string config_item("store." + cf_name);
    const auto cf_config = config->GetStringMap(config_item);
    if (cf_config.empty()) {
      continue;
    }

    for (const auto& [name, value] : cf_config) {
      column_family->SetConfItem(name, value);
    }
  }
}

template <typename T>
static bool CastValue(std::string value, T& dst_value) {
  if (value.empty()) {
    DINGO_LOG(FATAL) << fmt::format("[xdprocks] value is empty.");
    return false;
  }

  try {
    if (std::is_same_v<size_t, std::remove_reference_t<std::remove_cv_t<T>>>) {
      dst_value = std::stoul(value);
    } else if (std::is_same_v<int32_t, std::remove_reference_t<std::remove_cv_t<T>>>) {
      dst_value = std::stoi(value);
    } else if (std::is_same_v<uint32_t, std::remove_reference_t<std::remove_cv_t<T>>>) {
      dst_value = std::stoi(value);
    } else if (std::is_same_v<int64_t, std::remove_reference_t<std::remove_cv_t<T>>>) {
      dst_value = std::stoll(value);
    } else if (std::is_same_v<int64_t, std::remove_reference_t<std::remove_cv_t<T>>>) {
      dst_value = std::stoul(value);
    } else if (std::is_same_v<int, std::remove_reference_t<std::remove_cv_t<T>>>) {
      dst_value = std::stoi(value);
    } else if (std::is_same_v<float, std::remove_reference_t<std::remove_cv_t<T>>>) {
      dst_value = std::stof(value);
    } else if (std::is_same_v<double, std::remove_reference_t<std::remove_cv_t<T>>>) {
      dst_value = std::stod(value);
    } else {
      DINGO_LOG(FATAL) << fmt::format("[xdprocks] not match type failed, value: {}.", value);
      return false;
    }
  } catch (const std::invalid_argument& e) {
    DINGO_LOG(FATAL) << fmt::format("[xdprocks] cast type failed, value: {} error: {}.", value, e.what());
    return false;
  } catch (const std::out_of_range& e) {
    DINGO_LOG(FATAL) << fmt::format("[xdprocks] cast type failed, value: {} error: {}.", value, e.what());
    return false;
  }

  return true;
}

template <>
bool CastValue(std::string value, std::string& dst_value) {
  dst_value = value;
  return true;
}

// set cf config
static xdprocks::ColumnFamilyOptions GenRcoksDBColumnFamilyOptions(xdp::ColumnFamilyPtr column_family) {
  xdprocks::ColumnFamilyOptions family_options;
  xdprocks::BlockBasedTableOptions table_options;

  // block_size
  CastValue(column_family->GetConfItem(Constant::kBlockSize), table_options.block_size);

  // block_cache
  {
    size_t option_value = 0;
    CastValue(column_family->GetConfItem(Constant::kBlockCache), option_value);

    table_options.block_cache = xdprocks::NewLRUCache(option_value);  // LRUcache
  }

  // arena_block_size
  CastValue(column_family->GetConfItem(Constant::kArenaBlockSize), family_options.arena_block_size);

  // min_write_buffer_number_to_merge
  CastValue(column_family->GetConfItem(Constant::kMinWriteBufferNumberToMerge),
            family_options.min_write_buffer_number_to_merge);

  // max_write_buffer_number
  CastValue(column_family->GetConfItem(Constant::kMaxWriteBufferNumber), family_options.max_write_buffer_number);

  // max_compaction_bytes
  CastValue(column_family->GetConfItem(Constant::kMaxCompactionBytes), family_options.max_compaction_bytes);

  // write_buffer_size
  CastValue(column_family->GetConfItem(Constant::kWriteBufferSize), family_options.write_buffer_size);

  // max_bytes_for_level_multiplier
  CastValue(column_family->GetConfItem(Constant::kMaxBytesForLevelMultiplier),
            family_options.max_bytes_for_level_multiplier);

  // prefix_extractor
  {
    size_t value = 0;
    CastValue(column_family->GetConfItem(Constant::kPrefixExtractor), value);

    family_options.prefix_extractor.reset(xdprocks::NewCappedPrefixTransform(value));
  }

  // max_bytes_for_level_base
  CastValue(column_family->GetConfItem(Constant::kMaxBytesForLevelBase), family_options.max_bytes_for_level_base);

  // target_file_size_base
  CastValue(column_family->GetConfItem(Constant::kTargetFileSizeBase), family_options.target_file_size_base);

  family_options.compression_per_level = {
      xdprocks::CompressionType::kNoCompression,  xdprocks::CompressionType::kNoCompression,
      xdprocks::CompressionType::kLZ4Compression, xdprocks::CompressionType::kLZ4Compression,
      xdprocks::CompressionType::kLZ4Compression, xdprocks::CompressionType::kZSTD,
      xdprocks::CompressionType::kZSTD,
  };

  table_options.filter_policy.reset(xdprocks::NewBloomFilterPolicy(10.0, false));
  table_options.whole_key_filtering = true;

  xdprocks::TableFactory* table_factory = NewBlockBasedTableFactory(table_options);
  family_options.table_factory.reset(table_factory);

  return family_options;
}

static xdprocks::DB* InitDB(const std::string& db_path, xdp::ColumnFamilyMap& column_families) {
  // Cast ColumnFamily to xdprocks::ColumnFamilyOptions
  std::vector<xdprocks::ColumnFamilyDescriptor> column_family_descs;
  for (auto [cf_name, column_family] : column_families) {
    column_family->Dump();
    xdprocks::ColumnFamilyOptions family_options = GenRcoksDBColumnFamilyOptions(column_family);
    column_family_descs.push_back(xdprocks::ColumnFamilyDescriptor(cf_name, family_options));
  }

  xdprocks::DBOptions db_options;
  db_options.create_if_missing = true;
  db_options.create_missing_column_families = true;
  db_options.max_background_jobs = ConfigHelper::GetRocksDBBackgroundThreadNum();
  db_options.max_subcompactions = db_options.max_background_jobs / 4 * 3;
  db_options.stats_dump_period_sec = ConfigHelper::GetRocksDBStatsDumpPeriodSec();
  DINGO_LOG(INFO) << fmt::format("[xdprocks] config max_background_jobs({}) max_subcompactions({})",
                                 db_options.max_background_jobs, db_options.max_subcompactions);

  // Disable xdp kv fs
  // db_options.pliops_db_options.wal_storage_location = xdprocks::WALStorageLocation::FS;

  std::vector<std::string> configured_options;
  db_options.OptimizeForXdpRocks(&configured_options);
  for (const auto& option : configured_options) {
    DINGO_LOG(INFO) << fmt::format("[xdprocks] optimize option: {}", option);
  }

  xdprocks::DB* db;
  std::vector<xdprocks::ColumnFamilyHandle*> family_handles;
  xdprocks::Status s = xdprocks::DB::Open(db_options, db_path, column_family_descs, &family_handles, &db);
  if (!s.ok()) {
    DINGO_LOG(ERROR) << fmt::format("[xdprocks] open db failed, error: {}", s.ToString());
    return nullptr;
  }

  // Set family handle
  int i = 0;
  for (auto [_, column_family] : column_families) {
    column_family->SetHandle(family_handles[i++]);
  }

  return db;
}

// load xdprocks config from config file
bool XDPRocksRawEngine::Init(std::shared_ptr<Config> config, const std::vector<std::string>& cf_names) {
  DINGO_LOG(INFO) << "Init xdprocks raw engine...";
  if (BAIDU_UNLIKELY(!config)) {
    DINGO_LOG(ERROR) << fmt::format("[xdprocks] config empty not support!");
    return false;
  }

  std::string db_path = config->GetString(Constant::kStorePathConfigName) + "/xdprocks";
  if (BAIDU_UNLIKELY(db_path.empty())) {
    DINGO_LOG(ERROR) << fmt::format("[xdprocks] can not find: {}/xdprocks", Constant::kStorePathConfigName);
    return false;
  }

  db_path_ = db_path;
  DINGO_LOG(INFO) << fmt::format("[xdprocks] db path: {}", db_path_);

  // Column family config priority custom(store.$cf_name) > custom(store.base) > default.
  auto column_families = GenColumnFamilyByDefaultConfig(cf_names);
  SetColumnFamilyCustomConfig(config, column_families);

  xdprocks::DB* db = InitDB(db_path_, column_families);
  if (db == nullptr) {
    DINGO_LOG(ERROR) << fmt::format("[xdprocks] open failed, path: {}", db_path_);
    return false;
  }
  column_families_ = column_families;
  db_.reset(db);

  reader_ = std::make_shared<xdp::Reader>(GetSelfPtr());
  writer_ = std::make_shared<xdp::Writer>(GetSelfPtr());

  DINGO_LOG(INFO) << fmt::format("[xdprocks] open success, path: {}", db_path_);

  return true;
}

std::shared_ptr<XDPRocksRawEngine> XDPRocksRawEngine::GetSelfPtr() {
  return std::dynamic_pointer_cast<XDPRocksRawEngine>(shared_from_this());
}

std::string XDPRocksRawEngine::GetName() { return pb::common::RawEngine_Name(pb::common::RAW_ENG_XDPROCKS); }

pb::common::RawEngine XDPRocksRawEngine::GetRawEngineType() { return pb::common::RawEngine::RAW_ENG_XDPROCKS; }

std::string XDPRocksRawEngine::DbPath() { return db_path_; }

std::shared_ptr<xdprocks::DB> XDPRocksRawEngine::GetDB() { return db_; }

xdp::ColumnFamilyPtr XDPRocksRawEngine::GetDefaultColumnFamily() { return GetColumnFamily(Constant::kStoreDataCF); }

xdp::ColumnFamilyPtr XDPRocksRawEngine::GetColumnFamily(const std::string& cf_name) {
  auto it = column_families_.find(cf_name);
  if (it == column_families_.end()) {
    DINGO_LOG(FATAL) << fmt::format("[xdprocks] Not found column family {}", cf_name);
  }

  return it->second;
}

std::vector<xdp::ColumnFamilyPtr> XDPRocksRawEngine::GetColumnFamilies(const std::vector<std::string>& cf_names) {
  std::vector<xdp::ColumnFamilyPtr> column_families;
  column_families.reserve(cf_names.size());
  for (const auto& cf_name : cf_names) {
    auto column_family = GetColumnFamily(cf_name);
    if (column_family != nullptr) {
      column_families.push_back(column_family);
    }
  }

  return column_families;
}

dingodb::SnapshotPtr XDPRocksRawEngine::GetSnapshot() {
  return std::make_shared<xdp::Snapshot>(db_->GetSnapshot(), db_);
}

RawEngine::ReaderPtr XDPRocksRawEngine::Reader() { return reader_; }

RawEngine::WriterPtr XDPRocksRawEngine::Writer() { return writer_; }

xdp::SstFileWriterPtr XDPRocksRawEngine::NewSstFileWriter() {
  return std::make_shared<xdp::SstFileWriter>(xdprocks::Options());
}

RawEngine::CheckpointPtr XDPRocksRawEngine::NewCheckpoint() { return std::make_shared<xdp::Checkpoint>(GetSelfPtr()); }

butil::Status XDPRocksRawEngine::MergeCheckpointFiles(const std::string& path, const pb::common::Range& range,
                                                      const std::vector<std::string>& cf_names,
                                                      std::vector<std::string>& merge_sst_paths) {
  xdprocks::Options options;
  options.create_if_missing = false;

  if (cf_names.size() != merge_sst_paths.size()) {
    DINGO_LOG(ERROR) << fmt::format(
        "[xdprocks] merge checkpoint files failed, cf_names size: {}, merge_sst_paths size: {}", cf_names.size(),
        merge_sst_paths.size());
    return butil::Status(pb::error::EINTERNAL,
                         fmt::format("merge checkpoint files failed, cf_names size: {}, merge_sst_paths size: {}",
                                     cf_names.size(), merge_sst_paths.size()));
  }

  if (cf_names.empty()) {
    DINGO_LOG(ERROR) << fmt::format("[xdprocks] merge checkpoint files failed, cf_names empty");
    return butil::Status(pb::error::EINTERNAL, "merge checkpoint files failed, cf_names empty");
  }

  std::vector<xdprocks::ColumnFamilyDescriptor> column_families;
  column_families.reserve(cf_names.size());
  for (const auto& cf_name : cf_names) {
    column_families.push_back(xdprocks::ColumnFamilyDescriptor(cf_name, xdprocks::ColumnFamilyOptions()));
  }

  // Due to delete other region sst file, so need repair db, or xdprocks::DB::Open will fail.
  auto status = xdprocks::RepairDB(path, options, column_families);
  if (!status.ok()) {
    DINGO_LOG(WARNING) << fmt::format("[xdprocks] repair db failed, path: {} error: {}", path, status.ToString());
    return butil::Status(pb::error::EINTERNAL, fmt::format("Rocksdb Repair db failed, {}", status.ToString()));
  }

  auto default_cf_desc = xdprocks::ColumnFamilyDescriptor(Constant::kStoreDataCF, xdprocks::ColumnFamilyOptions());

  for (int i = 0; i < cf_names.size(); i++) {
    std::vector<xdprocks::ColumnFamilyDescriptor> cf_descs;
    cf_descs.push_back(default_cf_desc);
    if (cf_names[i] != Constant::kStoreDataCF) {
      cf_descs.push_back(xdprocks::ColumnFamilyDescriptor(cf_names[i], xdprocks::ColumnFamilyOptions()));
    }

    // Open snapshot db.
    xdprocks::DB* snapshot_db = nullptr;
    std::vector<xdprocks::ColumnFamilyHandle*> handles;
    status = xdprocks::DB::OpenForReadOnly(options, path, cf_descs, &handles, &snapshot_db);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << fmt::format("[xdprocks] open checkpoint failed, path: {} error: {}", path, status.ToString());
      // return butil::Status(pb::error::EINTERNAL, fmt::format("Rocksdb open checkpoint failed, {}",
      // status.ToString()));
      merge_sst_paths[i] = "";
      continue;
    }

    DINGO_LOG(INFO) << fmt::format("[xdprocks] open checkpoint success, path: {} cf_name: {}", path, cf_names[i]);

    // Create iterator
    IteratorOptions iter_options;
    iter_options.upper_bound = range.end_key();

    xdprocks::ReadOptions read_options;
    read_options.auto_prefix_mode = true;

    auto& merge_sst_path = merge_sst_paths[i];
    auto* handle = handles[0];
    if (handles.size() > 1) {
      handle = handles[1];
    }

    butil::Status ret_status = butil::Status::OK();
    {
      auto iter = std::make_shared<xdp::Iterator>(iter_options, snapshot_db->NewIterator(read_options, handle));
      if (iter == nullptr) {
        DINGO_LOG(ERROR) << fmt::format("[xdprocks] merge checkpoint files failed, create iterator failed");
        ret_status = butil::Status(pb::error::EINTERNAL, "merge checkpoint files failed, create iterator failed");
      } else {
        iter->Seek(range.start_key());
        auto ret = NewSstFileWriter()->SaveFile(iter, merge_sst_path);
        if (ret.error_code() == pb::error::Errno::ENO_ENTRIES) {
          DINGO_LOG(WARNING) << "[xdprocks] merge checkpoint files no entries, file_name=" << merge_sst_path;
          merge_sst_paths[i] = "";
        } else if (!ret.ok()) {
          DINGO_LOG(ERROR) << fmt::format("[xdprocks] merge checkpoint files failed, save file failed")
                           << ", error: " << ret.error_str();
          ret_status = butil::Status(pb::error::EINTERNAL, "merge checkpoint files failed, save file failed");
        }

        DINGO_LOG(INFO) << fmt::format("[xdprocks] merge checkpoint files success, path: {} cf_name: {}", path,
                                       cf_names[i]);
      }
    }

    // Close snapshot db.
    try {
      CancelAllBackgroundWork(snapshot_db, true);
      snapshot_db->DropColumnFamilies(handles);
      for (auto& handle : handles) {
        snapshot_db->DestroyColumnFamilyHandle(handle);
      }
      snapshot_db->Close();
      delete snapshot_db;
    } catch (std::exception& e) {
      DINGO_LOG(ERROR) << fmt::format("[xdprocks] close snapshot db failed, path: {} error: {}", path, e.what());
      ret_status = butil::Status(pb::error::EINTERNAL, fmt::format("Rocksdb close snapshot db failed, {}", e.what()));
    }

    if (!ret_status.ok()) {
      return ret_status;
    }
  }

  return butil::Status::OK();
}

butil::Status XDPRocksRawEngine::IngestExternalFile(const std::string& cf_name, const std::vector<std::string>& files) {
  xdprocks::IngestExternalFileOptions options;
  options.write_global_seqno = false;
  auto status = db_->IngestExternalFile(GetColumnFamily(cf_name)->GetHandle(), files, options);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("[xdprocks] ingest external fille failed, error: {}", status.ToString());
    return butil::Status(status.code(), status.ToString());
  }

  return butil::Status();
}

void XDPRocksRawEngine::Flush(const std::string& cf_name) {
  if (db_) {
    xdprocks::FlushOptions flush_options;
    db_->Flush(flush_options, GetColumnFamily(cf_name)->GetHandle());
  }
}

butil::Status XDPRocksRawEngine::Compact(const std::string& cf_name) {
  DINGO_LOG(INFO) << fmt::format("[xdprocks] compact column family {}", cf_name);
  if (db_ != nullptr) {
    xdprocks::CompactRangeOptions options;
    options.exclusive_manual_compaction = true;
    options.allow_write_stall = true;
    auto status = db_->CompactRange(options, GetColumnFamily(cf_name)->GetHandle(), nullptr, nullptr);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << fmt::format("[xdprocks] compact failed, column family {}", cf_name);
      return butil::Status(pb::error::EINTERNAL, "Compact column family %s failed", cf_name.c_str());
    }
  }

  return butil::Status();
}

void XDPRocksRawEngine::Destroy() { xdprocks::DestroyDB(db_path_, xdprocks::Options()); }

void XDPRocksRawEngine::Close() {
  if (db_) {
    CancelAllBackgroundWork(db_.get(), true);

    std::vector<xdprocks::ColumnFamilyHandle*> column_family_handles;
    for (auto& [_, column_family] : column_families_) {
      column_family_handles.push_back(column_family->GetHandle());
    }
    db_->DropColumnFamilies(column_family_handles);
    for (auto& handle : column_family_handles) {
      db_->DestroyColumnFamilyHandle(handle);
    }
    for (auto& [_, column_family] : column_families_) {
      column_family->SetHandle(nullptr);
    }

    db_->Close();
    db_ = nullptr;
  }

  DINGO_LOG(INFO) << fmt::format("[xdprocks] close db.");
}

std::vector<int64_t> XDPRocksRawEngine::GetApproximateSizes(const std::string& cf_name,
                                                            std::vector<pb::common::Range>& ranges) {
  xdprocks::SizeApproximationOptions options;

  xdprocks::Range inner_ranges[ranges.size()];
  for (int i = 0; i < ranges.size(); ++i) {
    inner_ranges[i].start = ranges[i].start_key();
    inner_ranges[i].limit = ranges[i].end_key();
  }

  uint64_t sizes[ranges.size()];
  db_->GetApproximateSizes(options, GetColumnFamily(cf_name)->GetHandle(), inner_ranges, ranges.size(), sizes);

  std::vector<int64_t> result;
  result.reserve(ranges.size());
  for (int i = 0; i < ranges.size(); ++i) {
    result.push_back(sizes[i]);
  }

  return result;
}

}  // namespace dingodb
