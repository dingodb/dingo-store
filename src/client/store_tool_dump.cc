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

#include "client/store_tool_dump.h"

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

#include "client/store_client_function.h"
#include "common/constant.h"
#include "common/helper.h"
#include "common/logging.h"
#include "coprocessor/utils.h"
#include "fmt/core.h"
#include "fmt/format.h"
#include "gflags/gflags.h"
#include "proto/common.pb.h"
#include "proto/meta.pb.h"
#include "rocksdb/db.h"
#include "rocksdb/listener.h"
#include "rocksdb/options.h"
#include "serial/record_decoder.h"
#include "serial/record_encoder.h"
#include "vector/codec.h"

DEFINE_bool(show_lock, false, "show lock info");
DEFINE_bool(show_write, false, "show write info");
DEFINE_bool(show_last_data, true, "show visible last data");
DEFINE_bool(show_all_data, false, "show all version data");

DEFINE_int32(print_column_width, 24, "print column width");

DECLARE_bool(show_pretty);

namespace client {

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

    int count = 0;
    std::string_view end_key_view(end_key);
    rocksdb::Iterator* it = db_->NewIterator(read_option, GetFamilyHandle(cf_name));
    for (it->Seek(begin_key); it->Valid(); it->Next()) {
      if (--offset >= 0) {
        continue;
      }
      ++count;
      // DINGO_LOG(INFO) << fmt::format("key: {} value: {}", it->key().ToString(true), it->value().ToString(true));
      handler(it->key().ToString(), it->value().ToString());
      if (--limit <= 0) {
        break;
      }
    }

    std::cout << fmt::format("Total row count: {}", count) << std::endl;

    delete it;
  }

 private:
  std::string db_path_;
  rocksdb::Options db_options_;
  std::shared_ptr<rocksdb::DB> db_;
  std::vector<std::string> family_names_;
  std::map<std::string, rocksdb::ColumnFamilyHandle*> family_handles_;
};

using RocksDBOperatorPtr = std::shared_ptr<RocksDBOperator>;

dingodb::pb::common::Schema::Type TransformSchemaType(const std::string& name) {
  static std::map<std::string, dingodb::pb::common::Schema::Type> schema_type_map = {
      std::make_pair("CHAR", dingodb::pb::common::Schema::STRING),
      std::make_pair("VARCHAR", dingodb::pb::common::Schema::STRING),
      std::make_pair("ANY", dingodb::pb::common::Schema::STRING),
      std::make_pair("BINARY", dingodb::pb::common::Schema::STRING),
      std::make_pair("INTEGER", dingodb::pb::common::Schema::INTEGER),
      std::make_pair("BIGINT", dingodb::pb::common::Schema::LONG),
      std::make_pair("DATE", dingodb::pb::common::Schema::LONG),
      std::make_pair("TIME", dingodb::pb::common::Schema::LONG),
      std::make_pair("TIMESTAMP", dingodb::pb::common::Schema::LONG),
      std::make_pair("DOUBLE", dingodb::pb::common::Schema::DOUBLE),
      std::make_pair("BOOL", dingodb::pb::common::Schema::BOOL),
      std::make_pair("BOOLEAN", dingodb::pb::common::Schema::BOOL),
      std::make_pair("FLOAT", dingodb::pb::common::Schema::FLOAT),
      std::make_pair("LONG", dingodb::pb::common::Schema::LONG),

      std::make_pair("ARRAY_BOOL", dingodb::pb::common::Schema::BOOLLIST),
      std::make_pair("ARRAY_BOOLEAN", dingodb::pb::common::Schema::BOOLLIST),
      std::make_pair("ARRAY_INTEGER", dingodb::pb::common::Schema::INTEGERLIST),
      std::make_pair("ARRAY_FLOAT", dingodb::pb::common::Schema::FLOATLIST),
      std::make_pair("ARRAY_DOUBLE", dingodb::pb::common::Schema::DOUBLELIST),
      std::make_pair("ARRAY_LONG", dingodb::pb::common::Schema::LONGLIST),
      std::make_pair("ARRAY_BIGINT", dingodb::pb::common::Schema::LONGLIST),
      std::make_pair("ARRAY_DATE", dingodb::pb::common::Schema::LONGLIST),
      std::make_pair("ARRAY_TIME", dingodb::pb::common::Schema::LONGLIST),
      std::make_pair("ARRAY_TIMESTAMP", dingodb::pb::common::Schema::LONGLIST),
      std::make_pair("ARRAY_CHAR", dingodb::pb::common::Schema::STRINGLIST),
      std::make_pair("ARRAY_VARCHAR", dingodb::pb::common::Schema::STRINGLIST),

      std::make_pair("MULTISET_BOOL", dingodb::pb::common::Schema::BOOLLIST),
      std::make_pair("MULTISET_BOOLEAN", dingodb::pb::common::Schema::BOOLLIST),
      std::make_pair("MULTISET_INTEGER", dingodb::pb::common::Schema::INTEGERLIST),
      std::make_pair("MULTISET_FLOAT", dingodb::pb::common::Schema::FLOATLIST),
      std::make_pair("MULTISET_DOUBLE", dingodb::pb::common::Schema::DOUBLELIST),
      std::make_pair("MULTISET_LONG", dingodb::pb::common::Schema::LONGLIST),
      std::make_pair("MULTISET_BIGINT", dingodb::pb::common::Schema::LONGLIST),
      std::make_pair("MULTISET_DATE", dingodb::pb::common::Schema::LONGLIST),
      std::make_pair("MULTISET_TIME", dingodb::pb::common::Schema::LONGLIST),
      std::make_pair("MULTISET_TIMESTAMP", dingodb::pb::common::Schema::LONGLIST),
      std::make_pair("MULTISET_CHAR", dingodb::pb::common::Schema::STRINGLIST),
      std::make_pair("MULTISET_VARCHAR", dingodb::pb::common::Schema::STRINGLIST),
  };

  auto it = schema_type_map.find(name);
  if (it == schema_type_map.end()) {
    DINGO_LOG(FATAL) << "Not found schema type: " << name;
  }

  return it->second;
}

std::vector<dingodb::pb::common::Schema> TransformColumnSchema(const dingodb::pb::meta::TableDefinition& definition) {
  std::vector<dingodb::pb::common::Schema> column_schemas;
  int i = 0;
  for (const auto& column : definition.columns()) {
    dingodb::pb::common::Schema schema;
    std::string sql_type = column.sql_type();
    if (sql_type == "ARRAY" || sql_type == "MULTISET") {
      sql_type += "_" + column.element_type();
    }
    schema.set_type(TransformSchemaType(sql_type));
    schema.set_index(i++);
    if (column.indexofkey() >= 0) {
      schema.set_is_key(true);
    }
    schema.set_is_nullable(column.nullable());
    column_schemas.push_back(schema);
  }

  return column_schemas;
}

template <typename T>
std::string FormatVecotr(std::vector<T>& vec) {
  std::stringstream str;
  for (int i = 0; i < vec.size(); ++i) {
    str << vec[i];
    if (i + 1 < vec.size()) {
      str << ",";
    }
  }

  return str.str();
}

std::string ConvertTOString(const dingodb::pb::meta::ColumnDefinition& column_definition,
                            const std::any& value) {  // NOLINT
  std::ostringstream ostr;

  if (value.type() == typeid(std::optional<std::string>)) {
    auto v = std::any_cast<std::optional<std::string>>(value);
    ostr << v.value_or("");
  } else if (value.type() == typeid(std::optional<std::shared_ptr<std::string>>)) {
    auto v = std::any_cast<std::optional<std::shared_ptr<std::string>>>(value);
    auto ptr = v.value_or(nullptr);
    if (ptr != nullptr) {
      if (column_definition.sql_type() == "BINARY" || column_definition.sql_type() == "ANY") {
        ostr << dingodb::Helper::StringToHex(*ptr);
      } else {
        ostr << *ptr;
      }
    }
  } else if (value.type() == typeid(std::optional<int32_t>)) {
    auto v = std::any_cast<std::optional<int32_t>>(value);
    ostr << v.value_or(0);
  } else if (value.type() == typeid(std::optional<uint32_t>)) {
    auto v = std::any_cast<std::optional<uint32_t>>(value);
    ostr << v.value_or(0);
  } else if (value.type() == typeid(std::optional<int64_t>)) {
    auto v = std::any_cast<std::optional<int64_t>>(value);
    ostr << v.value_or(0);
  } else if (value.type() == typeid(std::optional<int64_t>)) {
    auto v = std::any_cast<std::optional<int64_t>>(value);
    ostr << v.value_or(0);
  } else if (value.type() == typeid(std::optional<double>)) {
    auto v = std::any_cast<std::optional<double>>(value);
    ostr << v.value_or(0.0);
  } else if (value.type() == typeid(std::optional<float>)) {
    auto v = std::any_cast<std::optional<float>>(value);
    ostr << v.value_or(0.0);
  } else if (value.type() == typeid(std::optional<bool>)) {
    auto v = std::any_cast<std::optional<bool>>(value);
    ostr << v.value_or(false);

  } else if (value.type() == typeid(std::optional<std::shared_ptr<std::vector<bool>>>)) {
    auto v = std::any_cast<std::optional<std::shared_ptr<std::vector<bool>>>>(value);
    auto ptr = v.value_or(nullptr);
    if (ptr != nullptr) {
      ostr << FormatVecotr(*ptr);
    }

  } else if (value.type() == typeid(std::optional<std::shared_ptr<std::vector<std::string>>>)) {
    auto v = std::any_cast<std::optional<std::shared_ptr<std::vector<std::string>>>>(value);
    auto ptr = v.value_or(nullptr);
    if (ptr != nullptr) {
      ostr << FormatVecotr(*ptr);
    }

  } else if (value.type() == typeid(std::optional<std::shared_ptr<std::vector<double>>>)) {
    auto v = std::any_cast<std::optional<std::shared_ptr<std::vector<double>>>>(value);
    auto ptr = v.value_or(nullptr);
    if (ptr != nullptr) {
      ostr << FormatVecotr(*ptr);
    }

  } else if (value.type() == typeid(std::optional<std::shared_ptr<std::vector<float>>>)) {
    auto v = std::any_cast<std::optional<std::shared_ptr<std::vector<float>>>>(value);
    auto ptr = v.value_or(nullptr);
    if (ptr != nullptr) {
      ostr << FormatVecotr(*ptr);
    }

  } else if (value.type() == typeid(std::optional<std::shared_ptr<std::vector<int32_t>>>)) {
    auto v = std::any_cast<std::optional<std::shared_ptr<std::vector<int32_t>>>>(value);
    auto ptr = v.value_or(nullptr);
    if (ptr != nullptr) {
      ostr << FormatVecotr(*ptr);
    }

  } else if (value.type() == typeid(std::optional<std::shared_ptr<std::vector<int64_t>>>)) {
    auto v = std::any_cast<std::optional<std::shared_ptr<std::vector<int64_t>>>>(value);
    auto ptr = v.value_or(nullptr);
    if (ptr != nullptr) {
      ostr << FormatVecotr(*ptr);
    }

  } else {
    ostr << fmt::format("unknown type({})", value.type().name());
  }

  return ostr.str();
}

void PrintValues(const dingodb::pb::meta::TableDefinition& table_definition, const std::vector<std::any>& values,
                 int64_t ts = 0) {  // NOLINT
  std::vector<std::string> str_values;
  if (ts > 0) {
    str_values.push_back(std::to_string(ts));
  }
  for (int i = 0; i < values.size(); ++i) {
    const auto& column_definition = table_definition.columns().at(i);

    std::string str = ConvertTOString(column_definition, values[i]);
    if (str.size() >= FLAGS_print_column_width) {
      str = str.substr(0, FLAGS_print_column_width - 3) + "...";
    }
    str_values.push_back(str);
  }

  std::cout << fmt::format("{}", fmt::join(str_values, " | ")) << std::endl;
}

void PrintValuesPretty(const dingodb::pb::meta::TableDefinition& table_definition, const std::vector<std::any>& values,
                       int64_t ts = 0) {  // NOLINT

  std::cout << "****************************************************************" << std::endl;
  if (ts > 0) {
    std::cout << fmt::format("{:>32}: {:<64}", "TS", ts) << std::endl;
  }
  for (int i = 0; i < values.size(); ++i) {
    const auto& column_definition = table_definition.columns().at(i);

    std::string str = ConvertTOString(column_definition, values[i]);
    if (str.size() >= FLAGS_print_column_width) {
      str = str.substr(0, FLAGS_print_column_width - 3) + "...";
    }

    std::cout << fmt::format("{:>32}: {:<64}", column_definition.name(), str) << std::endl;
  }
}

std::string GetPrimaryString(const dingodb::pb::meta::TableDefinition& table_definition,
                             const std::vector<std::any>& values) {
  std::vector<std::string> result;
  for (int i = 0; i < values.size(); ++i) {
    if (strcmp(values[i].type().name(), "v") == 0) {
      continue;
    }
    const auto& column_definition = table_definition.columns().at(i);

    result.push_back(ConvertTOString(column_definition, values[i]));
  }

  return dingodb::Helper::VectorToString(result);
}

std::shared_ptr<std::vector<std::shared_ptr<dingodb::BaseSchema>>> GenSerialSchema(
    const dingodb::pb::meta::TableDefinition& definition) {
  auto column_schemas = TransformColumnSchema(definition);  // NOLINT
  google::protobuf::RepeatedPtrField<dingodb::pb::common::Schema> pb_schemas;
  dingodb::Helper::VectorToPbRepeated(column_schemas, &pb_schemas);

  auto serial_schemas = std::make_shared<std::vector<std::shared_ptr<dingodb::BaseSchema>>>();
  auto status = dingodb::Utils::TransToSerialSchema(pb_schemas, &serial_schemas);
  if (!status.ok()) {
    return nullptr;
  }

  return serial_schemas;
}

std::vector<int> GemSelectionColumnIndex(
    std::shared_ptr<std::vector<std::shared_ptr<dingodb::BaseSchema>>> serial_schemas) {
  std::vector<int> column_indexes;
  column_indexes.resize(serial_schemas->size(), -1);
  int i = 0;
  for (const auto& schema : *serial_schemas) {
    int index = schema->GetIndex();
    DINGO_LOG(DEBUG) << index << "," << i;
    column_indexes[index] = i;
    i++;
  }

  std::vector<int> selection_column_indexes;
  selection_column_indexes.reserve(column_indexes.size());
  for (auto index : column_indexes) {
    selection_column_indexes.push_back(index);
  }

  return selection_column_indexes;
}

void DumpExcutorRaw(std::shared_ptr<Context> ctx, dingodb::pb::meta::TableDefinition& table_definition,
                    const dingodb::pb::meta::Partition& partition) {
  auto db = std::make_shared<RocksDBOperator>(ctx->db_path, std::vector<std::string>{dingodb::Constant::kStoreDataCF});
  if (!db->Init()) {
    return;
  }

  auto serial_schema = GenSerialSchema(table_definition);
  auto record_encoder = std::make_shared<dingodb::RecordEncoder>(1, serial_schema, partition.id().entity_id());
  auto record_decoder = std::make_shared<dingodb::RecordDecoder>(1, serial_schema, partition.id().entity_id());

  std::string begin_key, end_key;
  record_encoder->EncodeMinKeyPrefix(dingodb::Constant::kExecutorRaw, begin_key);
  record_encoder->EncodeMaxKeyPrefix(dingodb::Constant::kExecutorRaw, end_key);

  auto row_handler = [&](const std::string& key, const std::string& value) {
    std::vector<std::any> record;
    int ret = record_decoder->Decode(key, value, record);
    if (ret != 0) {
      LOG(INFO) << fmt::format("Decode failed, ret: {} record size: {}", ret, record.size());
    }

    if (FLAGS_show_pretty) {
      PrintValuesPretty(table_definition, record);
    } else {
      PrintValues(table_definition, record);
    }
  };

  db->Scan(dingodb::Constant::kTxnDataCF, begin_key, end_key, ctx->offset, ctx->limit, row_handler);
}

void DumpExcutorTxn(std::shared_ptr<Context> ctx, dingodb::pb::meta::TableDefinition& table_definition,
                    const dingodb::pb::meta::Partition& partition) {
  auto db = std::make_shared<RocksDBOperator>(
      ctx->db_path, std::vector<std::string>{dingodb::Constant::kStoreDataCF, dingodb::Constant::kTxnDataCF,
                                             dingodb::Constant::kTxnLockCF, dingodb::Constant::kTxnWriteCF});
  if (!db->Init()) {
    return;
  }

  auto serial_schema = GenSerialSchema(table_definition);
  auto record_encoder = std::make_shared<dingodb::RecordEncoder>(1, serial_schema, partition.id().entity_id());
  auto record_decoder = std::make_shared<dingodb::RecordDecoder>(1, serial_schema, partition.id().entity_id());

  std::string begin_key, end_key;
  record_encoder->EncodeMinKeyPrefix(dingodb::Constant::kExecutorTxn, begin_key);
  record_encoder->EncodeMaxKeyPrefix(dingodb::Constant::kExecutorTxn, end_key);
  begin_key = dingodb::Helper::EncodeTxnKey(begin_key, 0);
  end_key = dingodb::Helper::EncodeTxnKey(end_key, 0);

  auto lock_handler = [&](const std::string& key, const std::string& value) {
    std::string origin_key;
    int64_t ts;
    auto status = dingodb::Helper::DecodeTxnKey(key, origin_key, ts);
    if (!status.ok()) {
      LOG(INFO) << "decoce txn key failed, error: " << status.error_str();
      return;
    }

    std::vector<std::any> record;
    int ret = record_decoder->DecodeKey(origin_key, record);
    if (ret != 0) {
      LOG(INFO) << fmt::format("Decode failed, ret: {} record size: {}", ret, record.size());
    }

    dingodb::pb::store::LockInfo lock_info;
    if (!lock_info.ParseFromString(value)) {
      LOG(ERROR) << "parse pb string failed.";
      return;
    }

    std::cout << fmt::format("lock key({}) ts({}) value({})", GetPrimaryString(table_definition, record), ts,
                             lock_info.ShortDebugString());
  };

  std::map<std::string, int64_t> last_datas;
  auto write_handler = [&](const std::string& key, const std::string& value) {
    std::string origin_key;
    int64_t ts;
    auto status = dingodb::Helper::DecodeTxnKey(key, origin_key, ts);
    if (!status.ok()) {
      LOG(INFO) << "decoce txn key failed, error: " << status.error_str();
      return;
    }

    std::vector<std::any> record;
    int ret = record_decoder->DecodeKey(origin_key, record);
    if (ret != 0) {
      LOG(INFO) << fmt::format("Decode failed, ret: {} record size: {}", ret, record.size());
    }

    dingodb::pb::store::WriteInfo write_info;
    if (!write_info.ParseFromString(value)) {
      LOG(ERROR) << "parse pb string failed.";
      return;
    }

    last_datas.insert(std::make_pair(origin_key, write_info.start_ts()));

    std::cout << fmt::format("write key({}) ts({}) value({})", GetPrimaryString(table_definition, record), ts,
                             write_info.ShortDebugString())
              << std::endl;
  };

  auto data_handler = [&](const std::string& key, const std::string& value) {
    std::string origin_key;
    int64_t ts;
    auto status = dingodb::Helper::DecodeTxnKey(key, origin_key, ts);
    if (!status.ok()) {
      LOG(INFO) << "decoce txn key failed, error: " << status.error_str();
      return;
    }

    if (FLAGS_show_last_data) {
      // filter not last ts data
      auto it = last_datas.find(origin_key);
      if (it != last_datas.end()) {
        int64_t last_ts = it->second;
        if (ts != last_ts) {
          return;
        }
      }
    }

    std::vector<std::any> record;
    int ret = record_decoder->Decode(origin_key, value, record);
    if (ret != 0) {
      LOG(INFO) << fmt::format("Decode failed, ret: {} record size: {}", ret, record.size());
    }
    if (FLAGS_show_pretty) {
      PrintValuesPretty(table_definition, record, ts);
    } else {
      PrintValues(table_definition, record, ts);
    }
  };

  std::cout << fmt::format("table_id({}) partition_id({}) range[{}, {})", partition.id().parent_entity_id(),
                           partition.id().entity_id(), dingodb::Helper::StringToHex(begin_key),
                           dingodb::Helper::StringToHex(end_key))
            << std::endl;

  if (FLAGS_show_lock) {
    std::cout << fmt::format("=================== lock ====================") << std::endl;
    db->Scan(dingodb::Constant::kTxnLockCF, begin_key, end_key, ctx->offset, ctx->limit, lock_handler);
  }

  if (FLAGS_show_write) {
    std::cout << fmt::format("=================== write ====================") << std::endl;
    db->Scan(dingodb::Constant::kTxnWriteCF, begin_key, end_key, ctx->offset, ctx->limit, write_handler);
  }

  if (FLAGS_show_all_data || FLAGS_show_last_data) {
    std::cout << fmt::format("=================== data ====================") << std::endl;
    db->Scan(dingodb::Constant::kTxnDataCF, begin_key, end_key, ctx->offset, ctx->limit, data_handler);
  }
}

void DumpClientRaw(std::shared_ptr<Context> ctx, dingodb::pb::meta::TableDefinition& table_definition,
                   const dingodb::pb::meta::Partition& partition) {
  auto db = std::make_shared<RocksDBOperator>(ctx->db_path, std::vector<std::string>{dingodb::Constant::kStoreDataCF});
  if (!db->Init()) {
    return;
  }

  // auto serial_schema = GenSerialSchema(table_definition);
  // auto record_encoder = std::make_shared<dingodb::RecordEncoder>(1, serial_schema, partition.id().entity_id());
  // auto record_decoder = std::make_shared<dingodb::RecordDecoder>(1, serial_schema, partition.id().entity_id());

  // std::string begin_key, end_key;
  // record_encoder->EncodeMinKeyPrefix(dingodb::Constant::kExecutorTxn, begin_key);
  // record_encoder->EncodeMaxKeyPrefix(dingodb::Constant::kExecutorTxn, end_key);

  std::string begin_key = partition.range().start_key();
  std::string end_key = partition.range().end_key();

  auto row_handler = [&](const std::string& key, const std::string& value) {
    LOG(INFO) << fmt::format("key: {} value: {}", dingodb::Helper::StringToHex(key),
                             dingodb::Helper::StringToHex(value));
  };

  db->Scan(dingodb::Constant::kTxnDataCF, begin_key, end_key, ctx->offset, ctx->limit, row_handler);
}

void DumpClientTxn(std::shared_ptr<Context> ctx, dingodb::pb::meta::TableDefinition& table_definition,
                   const dingodb::pb::meta::Partition& partition) {
  auto db = std::make_shared<RocksDBOperator>(
      ctx->db_path, std::vector<std::string>{dingodb::Constant::kStoreDataCF, dingodb::Constant::kTxnDataCF,
                                             dingodb::Constant::kTxnLockCF, dingodb::Constant::kTxnWriteCF});
  if (!db->Init()) {
    return;
  }

  std::string begin_key = dingodb::Helper::EncodeTxnKey(partition.range().start_key(), 0);
  std::string end_key = dingodb::Helper::EncodeTxnKey(partition.range().end_key(), 0);

  auto row_handler = [&](const std::string& key, const std::string& value) {
    LOG(INFO) << fmt::format("key: {} value: {}", dingodb::Helper::StringToHex(key),
                             dingodb::Helper::StringToHex(value));
  };

  db->Scan(dingodb::Constant::kTxnDataCF, begin_key, end_key, ctx->offset, ctx->limit, row_handler);
}

void DumpVectorIndexRaw(std::shared_ptr<Context> ctx, dingodb::pb::meta::TableDefinition& table_definition) {
  auto vector_data_handler = [&](const std::string& key, const std::string& value) {
    if (ctx->show_vector) {
      dingodb::pb::common::Vector data;
      data.ParseFromString(value);
      int dimension = data.float_values_size() > 0 ? data.float_values_size() : data.binary_values_size();
      std::cout << fmt::format("[vector data] vector_id({}) value: dimension({}) {}",
                               dingodb::VectorCodec::DecodeVectorId(key), dimension, FormatVector(data, 10))
                << std::endl;
    }
  };

  auto scalar_data_handler = [&](const std::string& key, const std::string& value) {
    if (ctx->show_vector) {
      dingodb::pb::common::VectorScalardata data;
      data.ParseFromString(value);
      std::cout << fmt::format("[scalar data] vector_id({}) value: {}", dingodb::VectorCodec::DecodeVectorId(key),
                               data.ShortDebugString())
                << std::endl;
    }
  };

  auto table_data_handler = [&](const std::string& key, const std::string& value) {
    if (ctx->show_vector) {
      dingodb::pb::common::VectorTableData data;
      data.ParseFromString(value);
      std::cout << fmt::format("[table data] vector_id({}) table_key: {} table_value: {}",
                               dingodb::VectorCodec::DecodeVectorId(key),
                               dingodb::Helper::StringToHex(data.table_key()),
                               dingodb::Helper::StringToHex(data.table_value()))
                << std::endl;
    }
  };

  // Read data from db
  auto db = std::make_shared<RocksDBOperator>(ctx->db_path, std::vector<std::string>{dingodb::Constant::kVectorDataCF});
  if (!db->Init()) {
    return;
  }

  for (const auto& partition : table_definition.table_partition().partitions()) {
    int64_t partition_id = partition.id().entity_id();

    std::string begin_key, end_key;
    char prefix = dingodb::Helper::GetKeyPrefix(partition.range().start_key());
    dingodb::VectorCodec::EncodeVectorKey(prefix, partition_id, 0, begin_key);
    dingodb::VectorCodec::EncodeVectorKey(prefix, partition_id, INT64_MAX, end_key);

    std::cout << fmt::format("=================== vector data partition({}) ====================", partition_id)
              << std::endl;
    db->Scan(dingodb::Constant::kVectorDataCF, begin_key, end_key, ctx->offset, ctx->limit, vector_data_handler);

    std::cout << fmt::format("=================== vector scalar data partition_id({}) ==========", partition_id)
              << std::endl;
    db->Scan(dingodb::Constant::kVectorScalarCF, begin_key, end_key, ctx->offset, ctx->limit, scalar_data_handler);

    std::cout << fmt::format("=================== vector table data partition_id({}) ============", partition_id)
              << std::endl;
    db->Scan(dingodb::Constant::kVectorTableCF, begin_key, end_key, ctx->offset, ctx->limit, table_data_handler);
  }
}

void DumpVectorIndexTxn(std::shared_ptr<Context> ctx, dingodb::pb::meta::TableDefinition& table_definition) {
  auto vector_data_handler = [&](const std::string& key, const std::string& value) {
    if (ctx->show_vector) {
      dingodb::pb::common::Vector data;
      data.ParseFromString(value);
      int dimension = data.float_values_size() > 0 ? data.float_values_size() : data.binary_values_size();
      std::cout << fmt::format("[vector data] vector_id({}) value: dimension({}) {}",
                               dingodb::VectorCodec::DecodeVectorId(key), dimension, FormatVector(data, 10))
                << std::endl;
    }
  };

  auto scalar_data_handler = [&](const std::string& key, const std::string& value) {
    if (ctx->show_vector) {
      dingodb::pb::common::VectorScalardata data;
      data.ParseFromString(value);
      std::cout << fmt::format("[scalar data] vector_id({}) value: {}", dingodb::VectorCodec::DecodeVectorId(key),
                               data.ShortDebugString())
                << std::endl;
    }
  };

  auto table_data_handler = [&](const std::string& key, const std::string& value) {
    if (ctx->show_vector) {
      dingodb::pb::common::VectorTableData data;
      data.ParseFromString(value);
      std::cout << fmt::format("[table data] vector_id({}) table_key: {} table_value: {}",
                               dingodb::VectorCodec::DecodeVectorId(key),
                               dingodb::Helper::StringToHex(data.table_key()),
                               dingodb::Helper::StringToHex(data.table_value()))
                << std::endl;
    }
  };

  // Read data from db
  auto db = std::make_shared<RocksDBOperator>(ctx->db_path, std::vector<std::string>{dingodb::Constant::kVectorDataCF});
  if (!db->Init()) {
    return;
  }

  for (const auto& partition : table_definition.table_partition().partitions()) {
    int64_t partition_id = partition.id().entity_id();

    std::string begin_key, end_key;
    char prefix = dingodb::Helper::GetKeyPrefix(partition.range().start_key());
    dingodb::VectorCodec::EncodeVectorKey(prefix, partition_id, 0, begin_key);
    dingodb::VectorCodec::EncodeVectorKey(prefix, partition_id, INT64_MAX, end_key);

    std::cout << fmt::format("=================== vector data partition({}) ====================", partition_id)
              << std::endl;
    db->Scan(dingodb::Constant::kVectorDataCF, begin_key, end_key, ctx->offset, ctx->limit, vector_data_handler);

    std::cout << fmt::format("=================== vector scalar data partition_id({}) ==========", partition_id)
              << std::endl;
    db->Scan(dingodb::Constant::kVectorScalarCF, begin_key, end_key, ctx->offset, ctx->limit, scalar_data_handler);

    std::cout << fmt::format("=================== vector table data partition_id({}) ============", partition_id)
              << std::endl;
    db->Scan(dingodb::Constant::kVectorTableCF, begin_key, end_key, ctx->offset, ctx->limit, table_data_handler);
  }
}

void DumpDb(std::shared_ptr<Context> ctx) {
  dingodb::pb::meta::TableDefinition table_definition;
  int64_t table_or_index_id = ctx->table_id > 0 ? ctx->table_id : ctx->index_id;
  if (table_or_index_id == 0) {
    DINGO_LOG(ERROR) << "table_id/index_id is invalid.";
    return;
  }
  table_definition = SendGetTable(table_or_index_id);
  if (table_definition.name().empty()) {
    table_definition = SendGetIndex(table_or_index_id);
  }
  if (table_definition.name().empty()) {
    DINGO_LOG(ERROR) << "not found table/index definition.";
    return;
  }

  std::cout << fmt::format("=========== Table/Index {} schema ===========", table_definition.name()) << std::endl;
  for (const auto& column : table_definition.columns()) {
    std::cout << "column: " << column.ShortDebugString() << std::endl;
  }
  std::cout << fmt::format("=========== Table/Index {} schema ===========", table_definition.name()) << std::endl;

  for (const auto& partition : table_definition.table_partition().partitions()) {
    auto index_type = table_definition.index_parameter().index_type();
    DINGO_LOG(INFO) << fmt::format("key prefix: {} index_type: {}",
                                   dingodb::Helper::GetKeyPrefix(partition.range().start_key()),
                                   dingodb::pb::common::IndexType_Name(index_type));

    if (dingodb::Helper::IsExecutorRaw(partition.range().start_key())) {
      if (index_type == dingodb::pb::common::INDEX_TYPE_NONE || index_type == dingodb::pb::common::INDEX_TYPE_SCALAR) {
        DumpExcutorRaw(ctx, table_definition, partition);
      } else if (index_type == dingodb::pb::common::INDEX_TYPE_VECTOR) {
        DumpVectorIndexRaw(ctx, table_definition);
      } else {
        DINGO_LOG(ERROR) << "not support index type.";
      }

    } else if (dingodb::Helper::IsExecutorTxn(partition.range().start_key())) {
      if (index_type == dingodb::pb::common::INDEX_TYPE_NONE || index_type == dingodb::pb::common::INDEX_TYPE_SCALAR) {
        DumpExcutorTxn(ctx, table_definition, partition);
      } else if (index_type == dingodb::pb::common::INDEX_TYPE_VECTOR) {
        DumpVectorIndexTxn(ctx, table_definition);
      } else {
        DINGO_LOG(ERROR) << "not support index type.";
      }

    } else if (dingodb::Helper::IsClientTxn(partition.range().start_key())) {
      // DumpClientTxn(ctx, table_definition, partition);

    } else if (dingodb::Helper::IsClientRaw(partition.range().start_key())) {
      if (index_type == dingodb::pb::common::INDEX_TYPE_NONE || index_type == dingodb::pb::common::INDEX_TYPE_SCALAR) {
        DumpClientRaw(ctx, table_definition, partition);
      } else if (index_type == dingodb::pb::common::INDEX_TYPE_VECTOR) {
        DumpVectorIndexRaw(ctx, table_definition);
      } else {
        DINGO_LOG(ERROR) << "not support index type.";
      }

    } else {
      DINGO_LOG(ERROR) << "unknown partition type, range is invalid.";
    }
  }
}

void DumpMeta(std::shared_ptr<Context> ctx) {}

}  // namespace client