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

#include "client_v2/dump.h"

#include <any>
#include <cstdint>
#include <cstring>
#include <functional>
#include <iostream>
#include <memory>
#include <optional>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "butil/status.h"
#include "client_v2/helper.h"
#include "client_v2/pretty.h"
#include "client_v2/store.h"
#include "common/constant.h"
#include "common/helper.h"
#include "common/logging.h"
#include "coprocessor/utils.h"
#include "document/codec.h"
#include "fmt/core.h"
#include "fmt/format.h"
#include "mvcc/codec.h"
#include "proto/common.pb.h"
#include "proto/meta.pb.h"
#include "rocksdb/db.h"
#include "rocksdb/listener.h"
#include "rocksdb/options.h"
#include "serial/record_decoder.h"
#include "serial/record_encoder.h"
#include "serial/utils.h"
#include "vector/codec.h"

namespace client_v2 {

std::string FormatVector(const dingodb::pb::common::Vector& data, int num) {
  std::string result = "[";
  int size = std::min(num, data.float_values_size());
  for (int i = 0; i < size; ++i) {
    if (data.value_type() == dingodb::pb::common::ValueType::FLOAT) {
      result += std::to_string(data.float_values(i));
    } else {
      result += dingodb::Helper::StringToHex(data.binary_values(i));
    }
    result += ",";
  }

  result += "...]";

  return result;
}

class RocksDBOperator;
using RocksDBOperatorPtr = std::shared_ptr<RocksDBOperator>;

class RocksDBOperator {
 public:
  RocksDBOperator(const std::string& db_path, const std::vector<std::string>& family_names)
      : db_path_(db_path), family_names_(family_names) {}
  ~RocksDBOperator() {
    for (auto [_, family_handle] : family_handles_) {
      delete family_handle;
    }

    db_->Close();
  }

  static RocksDBOperatorPtr New(const std::string& db_path, const std::vector<std::string>& family_names) {
    return std::make_shared<RocksDBOperator>(db_path, family_names);
  }

  bool Init() {
    rocksdb::DBOptions db_options;

    std::vector<rocksdb::ColumnFamilyDescriptor> column_families;
    for (const auto& family_name : family_names_) {
      rocksdb::ColumnFamilyOptions family_options;
      column_families.push_back(rocksdb::ColumnFamilyDescriptor(family_name, family_options));
    }

    std::vector<rocksdb::ColumnFamilyHandle*> family_handles;

    rocksdb::DB* db;
    rocksdb::Status s = rocksdb::DB::OpenForReadOnly(db_options, db_path_, column_families, &family_handles, &db);
    if (!s.ok()) {
      DINGO_LOG(ERROR) << fmt::format("Open db failed, error: {}", s.ToString());
      return false;
    }

    db_.reset(db);

    for (int i = 0; i < family_names_.size(); ++i) {
      family_handles_[family_names_[i]] = family_handles[i];
    }

    return true;
  }

  rocksdb::ColumnFamilyHandle* GetFamilyHandle(const std::string& family_name) {
    auto it = family_handles_.find(family_name);
    return (it == family_handles_.end()) ? nullptr : it->second;
  }

  void Scan(const std::string& cf_name, const std::string& begin_key, const std::string& end_key, int32_t offset,
            int32_t limit, std::function<void(const std::string&, const std::string&)> handler) {
    rocksdb::ReadOptions read_option;
    read_option.auto_prefix_mode = true;
    rocksdb::Slice end_key_slice(end_key);
    if (!end_key.empty()) {
      read_option.iterate_upper_bound = &end_key_slice;
    }

    std::string_view end_key_view(end_key);
    rocksdb::Iterator* it = db_->NewIterator(read_option, GetFamilyHandle(cf_name));
    for (it->Seek(begin_key); it->Valid(); it->Next()) {
      if (--offset >= 0) {
        continue;
      }

      handler(it->key().ToString(), it->value().ToString());
      if (--limit <= 0) {
        break;
      }
    }

    delete it;
  }

  std::vector<dingodb::pb::common::KeyValue> Scan(const std::string& cf_name, const std::string& begin_key,
                                                  const std::string& end_key, int32_t offset, int32_t limit) {
    rocksdb::ReadOptions read_option;
    read_option.auto_prefix_mode = true;
    rocksdb::Slice end_key_slice(end_key);
    if (!end_key.empty()) {
      read_option.iterate_upper_bound = &end_key_slice;
    }

    rocksdb::Iterator* it = db_->NewIterator(read_option, GetFamilyHandle(cf_name));
    std::vector<dingodb::pb::common::KeyValue> kvs;
    for (it->Seek(begin_key); it->Valid(); it->Next()) {
      if (--offset >= 0) {
        continue;
      }

      dingodb::pb::common::KeyValue kv;
      kv.set_key(it->key().ToString());
      kv.set_value(it->value().ToString());
      kvs.push_back(kv);

      if (--limit <= 0) {
        break;
      }
    }

    delete it;

    return kvs;
  }

 private:
  std::string db_path_;
  rocksdb::Options db_options_;
  std::shared_ptr<rocksdb::DB> db_;
  std::vector<std::string> family_names_;
  std::map<std::string, rocksdb::ColumnFamilyHandle*> family_handles_;
};

static dingodb::pb::debug::DumpRegionResponse::Data DumpTxn(DumpDbOptions const& opt,
                                                            dingodb::pb::meta::TableDefinition& table_definition) {
  auto db = std::make_shared<RocksDBOperator>(
      opt.db_path, std::vector<std::string>{dingodb::Constant::kStoreDataCF, dingodb::Constant::kTxnDataCF,
                                            dingodb::Constant::kTxnLockCF, dingodb::Constant::kTxnWriteCF});
  if (!db->Init()) {
    return {};
  }

  dingodb::pb::debug::DumpRegionResponse::Data data;
  auto* txn = data.mutable_txn();

  for (const auto& partition : table_definition.table_partition().partitions()) {
    dingodb::pb::common::Range range;
    range.set_start_key(dingodb::mvcc::Codec::EncodeBytes(partition.range().start_key()));
    range.set_end_key(dingodb::mvcc::Codec::EncodeBytes(partition.range().end_key()));

    // data
    {
      auto kvs = db->Scan(dingodb::Constant::kTxnDataCF, range.start_key(), range.end_key(), opt.offset, opt.limit);
      DINGO_LOG(INFO) << "data column family kv num: " << kvs.size();
      for (auto& kv : kvs) {
        auto* data = txn->add_datas();

        std::string decode_key;
        int64_t ts;
        dingodb::mvcc::Codec::DecodeKey(kv.key(), decode_key, ts);
        data->set_key(decode_key);
        data->set_ts(ts);

        data->set_value(std::string(kv.value()));
        data->set_partition_id(partition.id().entity_id());
      }
    }

    // lock
    {
      auto kvs = db->Scan(dingodb::Constant::kTxnLockCF, range.start_key(), range.end_key(), opt.offset, opt.limit);
      DINGO_LOG(INFO) << "lock column family kv num: " << kvs.size();
      for (const auto& kv : kvs) {
        auto* lock = txn->add_locks();

        std::string plain_key;
        int64_t ts = 0;
        dingodb::mvcc::Codec::DecodeKey(kv.key(), plain_key, ts);

        lock->set_key(plain_key);
        lock->mutable_lock_info()->ParseFromString(kv.value());

        lock->set_partition_id(partition.id().entity_id());
      }
    }

    // write
    {
      auto kvs = db->Scan(dingodb::Constant::kTxnWriteCF, range.start_key(), range.end_key(), opt.offset, opt.limit);
      DINGO_LOG(INFO) << "write column family kv num: " << kvs.size();
      for (const auto& kv : kvs) {
        auto* write = txn->add_writes();

        std::string plain_key;
        int64_t ts = 0;
        dingodb::mvcc::Codec::DecodeKey(kv.key(), plain_key, ts);

        write->set_key(plain_key);
        write->set_ts(ts);

        write->mutable_write_info()->ParseFromString(kv.value());

        write->set_partition_id(partition.id().entity_id());
      }
    }
  }

  return data;
}

dingodb::pb::debug::DumpRegionResponse::Data DumpRawKv(DumpDbOptions const& opt,
                                                       dingodb::pb::meta::TableDefinition& table_definition) {
  auto db = std::make_shared<RocksDBOperator>(opt.db_path, std::vector<std::string>{dingodb::Constant::kStoreDataCF});
  if (!db->Init()) {
    return {};
  }

  dingodb::pb::debug::DumpRegionResponse::Data data;
  auto* kvs = data.mutable_kvs();

  for (const auto& partition : table_definition.table_partition().partitions()) {
    auto row_handler = [&kvs](const std::string& encode_key, const std::string& pkg_value) {
      std::string decode_key;
      int64_t ts;
      dingodb::mvcc::Codec::DecodeKey(encode_key, decode_key, ts);
      dingodb::pb::debug::DumpRegionResponse::KV kv;
      kv.set_key(decode_key);
      kv.set_ts(ts);

      dingodb::mvcc::ValueFlag flag;
      int64_t ttl;
      auto value = dingodb::mvcc::Codec::UnPackageValue(pkg_value, flag, ttl);

      kv.set_flag(static_cast<dingodb::pb::debug::DumpRegionResponse::ValueFlag>(flag));
      kv.set_ttl(ttl);
      kv.set_value(std::string(value));

      kvs->Add()->Swap(&kv);
    };

    std::string begin_key = dingodb::mvcc::Codec::EncodeBytes(partition.range().start_key());
    std::string end_key = dingodb::mvcc::Codec::EncodeBytes(partition.range().end_key());

    db->Scan(dingodb::Constant::kStoreDataCF, begin_key, end_key, opt.offset, opt.limit, row_handler);
  }

  return std::move(data);
}

dingodb::pb::debug::DumpRegionResponse::Data DumpRawVectorIndex(DumpDbOptions const& opt,
                                                                dingodb::pb::meta::TableDefinition& table_definition) {
  // Read data from db
  auto db = std::make_shared<RocksDBOperator>(
      opt.db_path, std::vector<std::string>{dingodb::Constant::kVectorDataCF, dingodb::Constant::kVectorScalarCF,
                                            dingodb::Constant::kVectorTableCF});
  if (!db->Init()) {
    return {};
  }

  dingodb::pb::debug::DumpRegionResponse::Data data;
  auto* vectors = data.mutable_vectors();
  for (const auto& partition : table_definition.table_partition().partitions()) {
    dingodb::pb::common::Range range;
    range.set_start_key(dingodb::mvcc::Codec::EncodeKey(partition.range().start_key(), 0));
    range.set_end_key(dingodb::mvcc::Codec::EncodeKey(partition.range().end_key(), 0));

    std::vector<dingodb::pb::debug::DumpRegionResponse::Vector> part_vectors;

    // vector data
    {
      auto kvs = db->Scan(dingodb::Constant::kVectorDataCF, range.start_key(), range.end_key(), opt.offset, opt.limit);
      for (const auto& kv : kvs) {
        const auto& encode_key = std::string(kv.key());
        dingodb::pb::debug::DumpRegionResponse::Vector vector;
        vector.set_key(encode_key);

        int64_t ts = dingodb::VectorCodec::TruncateKeyForTs(encode_key);
        std::string encode_key_no_ts(dingodb::VectorCodec::TruncateTsForKey(encode_key));
        vector.set_vector_id(dingodb::VectorCodec::DecodeVectorIdFromEncodeKey(encode_key_no_ts));
        vector.set_ts(ts);

        dingodb::mvcc::ValueFlag flag;
        int64_t ttl;
        auto value = dingodb::mvcc::Codec::UnPackageValue(kv.value(), flag, ttl);

        vector.set_flag(static_cast<dingodb::pb::debug::DumpRegionResponse::ValueFlag>(flag));
        vector.set_ttl(ttl);

        if (flag == dingodb::mvcc::ValueFlag::kPut || flag == dingodb::mvcc::ValueFlag::kPutTTL) {
          if (!vector.mutable_vector()->ParseFromArray(value.data(), value.size())) {
            DINGO_LOG(FATAL) << fmt::format("Parse vector proto failed, value size: {}.", value.size());
          }
        }

        part_vectors.push_back(vector);
      }
    }

    // scalar data
    {
      auto kvs =
          db->Scan(dingodb::Constant::kVectorScalarCF, range.start_key(), range.end_key(), opt.offset, opt.limit);
      uint32_t count = 0;
      for (const auto& kv : kvs) {
        auto& vector = part_vectors.at(count++);
        CHECK(kv.key() == vector.key()) << "Not match key.";

        dingodb::mvcc::ValueFlag flag;
        int64_t ttl;
        auto value = dingodb::mvcc::Codec::UnPackageValue(kv.value(), flag, ttl);
        if (flag == dingodb::mvcc::ValueFlag::kPut || flag == dingodb::mvcc::ValueFlag::kPutTTL) {
          if (!vector.mutable_scalar_data()->ParseFromArray(value.data(), value.size())) {
            DINGO_LOG(FATAL) << fmt::format("Parse vector scalar proto failed, value size: {}.", value.size());
          }
        }
      }
    }

    // table data
    {
      auto kvs = db->Scan(dingodb::Constant::kVectorTableCF, range.start_key(), range.end_key(), opt.offset, opt.limit);
      uint32_t count = 0;
      for (const auto& kv : kvs) {
        auto& vector = part_vectors.at(count++);
        CHECK(kv.key() == vector.key()) << "Not match key.";

        dingodb::mvcc::ValueFlag flag;
        int64_t ttl;
        auto value = dingodb::mvcc::Codec::UnPackageValue(kv.value(), flag, ttl);
        if (flag == dingodb::mvcc::ValueFlag::kPut || flag == dingodb::mvcc::ValueFlag::kPutTTL) {
          if (!vector.mutable_table_data()->ParseFromArray(value.data(), value.size())) {
            DINGO_LOG(FATAL) << fmt::format("Parse vector table proto failed, value size: {}.", value.size());
          }
        }
      }
    }

    for (auto& vector : part_vectors) {
      *vectors->Add() = vector;
    }
  }

  return std::move(data);
}

dingodb::pb::debug::DumpRegionResponse::Data DumpRawDocumentIndex(
    DumpDbOptions const& opt, dingodb::pb::meta::TableDefinition& table_definition) {
  // Read data from db
  auto db = std::make_shared<RocksDBOperator>(opt.db_path, std::vector<std::string>{dingodb::Constant::kStoreDataCF});
  if (!db->Init()) {
    return {};
  }

  dingodb::pb::debug::DumpRegionResponse::Data data;
  auto* documents = data.mutable_documents();
  for (const auto& partition : table_definition.table_partition().partitions()) {
    dingodb::pb::common::Range range;
    range.set_start_key(dingodb::mvcc::Codec::EncodeKey(partition.range().start_key(), 0));
    range.set_end_key(dingodb::mvcc::Codec::EncodeKey(partition.range().end_key(), 0));

    auto kvs = db->Scan(dingodb::Constant::kStoreDataCF, range.start_key(), range.end_key(), opt.offset, opt.limit);

    for (const auto& kv : kvs) {
      std::string plain_key;
      int64_t partition_id;
      int64_t document_id;
      dingodb::DocumentCodec::DecodeFromEncodeKeyWithTs(kv.key(), partition_id, document_id);

      dingodb::pb::debug::DumpRegionResponse::Document document;
      document.set_document_id(document_id);
      document.set_ts(dingodb::DocumentCodec::TruncateKeyForTs(kv.key()));

      dingodb::mvcc::ValueFlag flag;
      int64_t ttl;
      auto value = dingodb::mvcc::Codec::UnPackageValue(kv.value(), flag, ttl);

      document.set_flag(static_cast<dingodb::pb::debug::DumpRegionResponse::ValueFlag>(flag));
      document.set_ttl(ttl);

      if (flag == dingodb::mvcc::ValueFlag::kPut || flag == dingodb::mvcc::ValueFlag::kPutTTL) {
        if (!document.mutable_document()->ParseFromArray(value.data(), value.size())) {
          DINGO_LOG(FATAL) << fmt::format("Parse document proto failed, value size: {}.", value.size());
        }
      }

      documents->Add()->Swap(&document);
    }
  }

  return std::move(data);
}

butil::Status DumpDb(DumpDbOptions const& opt) {
  dingodb::pb::meta::TableDefinition table_definition;
  if (opt.id == 0) {
    return butil::Status(1, "table_id/index_id is invalid");
  }

  table_definition = SendGetTable(opt.id);
  if (table_definition.name().empty()) {
    table_definition = SendGetIndex(opt.id);
  }
  if (table_definition.name().empty()) {
    return butil::Status(1, "Not found table/index");
  }

  auto index_type = table_definition.index_parameter().index_type();
  const auto& partition = table_definition.table_partition().partitions().at(0);
  const auto& range = partition.range();

  dingodb::pb::debug::DumpRegionResponse::Data data;
  if (dingodb::Helper::IsClientTxn(range.start_key()) || dingodb::Helper::IsExecutorTxn(range.start_key())) {
    data = DumpTxn(opt, table_definition);
  } else {
    if (index_type == dingodb::pb::common::INDEX_TYPE_NONE || index_type == dingodb::pb::common::INDEX_TYPE_SCALAR) {
      data = DumpRawKv(opt, table_definition);
    } else if (index_type == dingodb::pb::common::INDEX_TYPE_VECTOR) {
      data = DumpRawVectorIndex(opt, table_definition);
    } else if (index_type == dingodb::pb::common::INDEX_TYPE_DOCUMENT) {
      data = DumpRawDocumentIndex(opt, table_definition);
    }
  }

  std::vector<std::string> exclude_columns;
  dingodb::Helper::SplitString(opt.exclude_columns, ',', exclude_columns);
  Pretty::Show(data, table_definition, exclude_columns);

  return butil::Status();
}

}  // namespace client_v2