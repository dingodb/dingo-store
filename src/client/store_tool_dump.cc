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
#include <functional>
#include <memory>
#include <optional>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "client/store_client_function.h"
#include "common/helper.h"
#include "common/logging.h"
#include "coprocessor/utils.h"
#include "fmt/core.h"
#include "proto/meta.pb.h"
#include "proto/store.pb.h"
#include "rocksdb/db.h"
#include "rocksdb/listener.h"
#include "rocksdb/options.h"
#include "serial/record_decoder.h"
#include "serial/record_encoder.h"

namespace client {

class RocksDBOperator {
 public:
  RocksDBOperator(const std::string& db_path) : db_path_(db_path) {}
  ~RocksDBOperator() {
    for (auto [_, family_handle] : family_handles_) {
      delete family_handle;
    }

    db_->Close();
  }

  bool Init() {
    rocksdb::DBOptions db_options;

    std::vector<std::string> family_names = {"default", "meta"};
    std::vector<rocksdb::ColumnFamilyDescriptor> column_families;
    for (const auto& family_name : family_names) {
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

    for (int i = 0; i < family_names.size(); ++i) {
      family_handles_[family_names[i]] = family_handles[i];
    }

    return true;
  }

  rocksdb::ColumnFamilyHandle* GetFamilyHandle(const std::string& family_name) {
    auto it = family_handles_.find(family_name);
    return (it == family_handles_.end()) ? nullptr : it->second;
  }

  void Scan(const std::string& begin_key, const std::string& end_key, int32_t offset, int32_t limit,
            std::function<void(const std::string&, const std::string&)> handler) {
    rocksdb::ReadOptions read_option;
    read_option.auto_prefix_mode = true;
    rocksdb::Slice end_key_slice(end_key);
    if (!end_key.empty()) {
      read_option.iterate_upper_bound = &end_key_slice;
    }

    int count = 0;
    std::string_view end_key_view(end_key);
    rocksdb::Iterator* it = db_->NewIterator(read_option, GetFamilyHandle("default"));
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

    DINGO_LOG(INFO) << fmt::format("Row count({})", count);

    delete it;
  }

 private:
  std::string db_path_;
  rocksdb::Options db_options_;
  std::shared_ptr<rocksdb::DB> db_;
  std::map<std::string, rocksdb::ColumnFamilyHandle*> family_handles_;
};

dingodb::pb::store::Schema::Type TransformSchemaType(const std::string& name) {
  static std::map<std::string, dingodb::pb::store::Schema::Type> schema_type_map = {
      std::make_pair("CHAR", dingodb::pb::store::Schema::STRING),
      std::make_pair("VARCHAR", dingodb::pb::store::Schema::STRING),
      std::make_pair("INTEGER", dingodb::pb::store::Schema::INTEGER),
      std::make_pair("DOUBLE", dingodb::pb::store::Schema::DOUBLE),
      std::make_pair("BOOL", dingodb::pb::store::Schema::BOOL),
      std::make_pair("FLOAT", dingodb::pb::store::Schema::FLOAT),
      std::make_pair("LONG", dingodb::pb::store::Schema::LONG),
      std::make_pair("BOOLLIST", dingodb::pb::store::Schema::BOOLLIST),
      std::make_pair("INTEGERLIST", dingodb::pb::store::Schema::INTEGERLIST),
      std::make_pair("FLOATLIST", dingodb::pb::store::Schema::FLOATLIST),
      std::make_pair("LONGLIST", dingodb::pb::store::Schema::LONGLIST),
      std::make_pair("DOUBLELIST", dingodb::pb::store::Schema::DOUBLELIST),
      std::make_pair("STRINGLIST", dingodb::pb::store::Schema::STRINGLIST),
  };

  auto it = schema_type_map.find(name);
  if (it == schema_type_map.end()) {
    DINGO_LOG(FATAL) << "Not found schema type: " << name;
  }

  return it->second;
}

std::vector<dingodb::pb::store::Schema> TransformColumnSchema(const dingodb::pb::meta::TableDefinition& definition) {
  std::vector<dingodb::pb::store::Schema> column_schemas;
  int i = 0;
  for (const auto& column : definition.columns()) {
    dingodb::pb::store::Schema schema;
    schema.set_type(TransformSchemaType(column.sql_type()));
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

void PrintValue(const std::any value) {  // NOLINT
  if (value.type() == typeid(std::optional<std::string>)) {
    auto v = std::any_cast<std::optional<std::string>>(value);
    std::cout << v.value_or("");
  } else if (value.type() == typeid(std::optional<std::shared_ptr<std::string>>)) {
    auto v = std::any_cast<std::optional<std::shared_ptr<std::string>>>(value);
    auto ptr = v.value_or(nullptr);
    if (ptr != nullptr) {
      std::cout << *ptr;
    }
  } else if (value.type() == typeid(std::optional<int32_t>)) {
    auto v = std::any_cast<std::optional<int32_t>>(value);
    std::cout << v.value_or(0);
  } else if (value.type() == typeid(std::optional<uint32_t>)) {
    auto v = std::any_cast<std::optional<uint32_t>>(value);
    std::cout << v.value_or(0);
  } else if (value.type() == typeid(std::optional<int64_t>)) {
    auto v = std::any_cast<std::optional<int64_t>>(value);
    std::cout << v.value_or(0);
  } else if (value.type() == typeid(std::optional<uint64_t>)) {
    auto v = std::any_cast<std::optional<uint64_t>>(value);
    std::cout << v.value_or(0);
  } else if (value.type() == typeid(std::optional<double>)) {
    auto v = std::any_cast<std::optional<double>>(value);
    std::cout << v.value_or(0.0);
  } else if (value.type() == typeid(std::optional<float>)) {
    auto v = std::any_cast<std::optional<float>>(value);
    std::cout << v.value_or(0.0);
  } else if (value.type() == typeid(std::optional<bool>)) {
    auto v = std::any_cast<std::optional<bool>>(value);
    std::cout << v.value_or(false);

  } else if (value.type() == typeid(std::optional<std::shared_ptr<std::vector<bool>>>)) {
    auto v = std::any_cast<std::optional<std::shared_ptr<std::vector<bool>>>>(value);
    auto ptr = v.value_or(nullptr);
    if (ptr != nullptr) {
      std::cout << FormatVecotr(*ptr);
    }

  } else if (value.type() == typeid(std::optional<std::shared_ptr<std::vector<std::string>>>)) {
    auto v = std::any_cast<std::optional<std::shared_ptr<std::vector<std::string>>>>(value);
    auto ptr = v.value_or(nullptr);
    if (ptr != nullptr) {
      std::cout << FormatVecotr(*ptr);
    }

  } else if (value.type() == typeid(std::optional<std::shared_ptr<std::vector<double>>>)) {
    auto v = std::any_cast<std::optional<std::shared_ptr<std::vector<double>>>>(value);
    auto ptr = v.value_or(nullptr);
    if (ptr != nullptr) {
      std::cout << FormatVecotr(*ptr);
    }

  } else if (value.type() == typeid(std::optional<std::shared_ptr<std::vector<float>>>)) {
    auto v = std::any_cast<std::optional<std::shared_ptr<std::vector<float>>>>(value);
    auto ptr = v.value_or(nullptr);
    if (ptr != nullptr) {
      std::cout << FormatVecotr(*ptr);
    }

  } else if (value.type() == typeid(std::optional<std::shared_ptr<std::vector<int32_t>>>)) {
    auto v = std::any_cast<std::optional<std::shared_ptr<std::vector<int32_t>>>>(value);
    auto ptr = v.value_or(nullptr);
    if (ptr != nullptr) {
      std::cout << FormatVecotr(*ptr);
    }

  } else if (value.type() == typeid(std::optional<std::shared_ptr<std::vector<int64_t>>>)) {
    auto v = std::any_cast<std::optional<std::shared_ptr<std::vector<int64_t>>>>(value);
    auto ptr = v.value_or(nullptr);
    if (ptr != nullptr) {
      std::cout << FormatVecotr(*ptr);
    }

  } else {
    std::cout << "unknown type: " << value.type().name();
  }
}

void PrintValues(const std::vector<std::any>& values) {  // NOLINT
  for (int i = 0; i < values.size(); ++i) {
    PrintValue(values[i]);
    if (i + 1 < values.size()) {
      std::cout << " | ";
    }
  }
  std::cout << std::endl;
}

std::shared_ptr<std::vector<std::shared_ptr<dingodb::BaseSchema>>> GenSerialSchema(
    const dingodb::pb::meta::TableDefinition& definition) {
  auto column_schemas = TransformColumnSchema(definition);  // NOLINT
  google::protobuf::RepeatedPtrField<dingodb::pb::store::Schema> pb_schemas;
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

void DumpDb(std::shared_ptr<Context> ctx) {
  dingodb::pb::meta::TableDefinition table_definition;
  if (ctx->table_id > 0) {
    table_definition = SendGetTable(ctx->coordinator_interaction, ctx->table_id);
  } else if (ctx->index_id > 0) {
    table_definition = SendGetIndex(ctx->coordinator_interaction, ctx->index_id);
  }
  DINGO_LOG(INFO) << fmt::format("Table|Index {} definition ...", table_definition.name());
  for (const auto& column : table_definition.columns()) {
    DINGO_LOG(INFO) << "column: " << column.ShortDebugString();
  }

  for (const auto& partition : table_definition.table_partition().partitions()) {
    auto serial_schema = GenSerialSchema(table_definition);
    auto record_encoder = std::make_shared<dingodb::RecordEncoder>(0, serial_schema, partition.id().entity_id());
    auto record_decoder = std::make_shared<dingodb::RecordDecoder>(0, serial_schema, partition.id().entity_id());

    auto handler = [&](const std::string& key, const std::string& value) {
      std::vector<std::any> record;
      int ret = record_decoder->Decode(key, value, record);
      if (ret != 0) {
        LOG(INFO) << fmt::format("Decode failed, ret: {} record size: {}", ret, record.size());
      }
      PrintValues(record);
    };

    // Read data from db
    auto db = std::make_shared<RocksDBOperator>(ctx->db_path);
    if (!db->Init()) {
      return;
    }

    std::string begin_key, end_key;
    record_encoder->EncodeMinKeyPrefix(begin_key);
    record_encoder->EncodeMaxKeyPrefix(end_key);

    DINGO_LOG(INFO) << fmt::format("table_id|index_id({}) partition_id({}) range[{}, {})",
                                   partition.id().parent_entity_id(), partition.id().entity_id(),
                                   dingodb::Helper::StringToHex(begin_key), dingodb::Helper::StringToHex(end_key));

    db->Scan(begin_key, end_key, ctx->offset, ctx->limit, handler);
  }
}

}  // namespace client