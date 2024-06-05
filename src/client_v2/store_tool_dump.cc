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

#include "client_v2/store_tool_dump.h"

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

#include "client_v2/client_helper.h"
#include "client_v2/store_client_function.h"
// #include "common/constant.h"
// #include "common/helper.h"
// #include "common/logging.h"
// #include "coprocessor/utils.h"
// #include "fmt/core.h"
// #include "fmt/format.h"
// // #include "gflags/gflags.h"
// #include "proto/common.pb.h"
// #include "proto/meta.pb.h"
// #include "rocksdb/db.h"
// #include "rocksdb/listener.h"
// #include "rocksdb/options.h"
// #include "serial/record_decoder.h"
// #include "serial/record_encoder.h"
// #include "serial/utils.h"
// #include "vector/codec.h"

#include "common/constant.h"
#include "common/helper.h"
#include "common/logging.h"
#include "coprocessor/utils.h"
#include "fmt/core.h"
#include "fmt/format.h"
#include "gflags/gflags.h"
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
// DEFINE_bool(show_lock, false, "show lock info");
// DEFINE_bool(show_write, false, "show write info");
// DEFINE_bool(show_last_data, true, "show visible last data");
// DEFINE_bool(show_all_data, false, "show all version data");

// DEFINE_int32(print_column_width, 24, "print column width");

// DECLARE_bool(show_pretty);

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

void PrintValues(const dingodb::pb::meta::TableDefinition& table_definition, const std::vector<std::any>& values,
                 int32_t print_column_width, int64_t ts = 0) {  // NOLINT
  std::vector<std::string> str_values;
  if (ts > 0) {
    str_values.push_back(std::to_string(ts));
  }
  for (int i = 0; i < values.size(); ++i) {
    const auto& column_definition = table_definition.columns().at(i);

    std::string str = dingodb::Helper::ConvertColumnValueToString(column_definition, values[i]);
    if (str.size() >= print_column_width) {
      str = str.substr(0, print_column_width - 3) + "...";
    }
    str_values.push_back(str);
  }

  std::cout << fmt::format("{}", fmt::join(str_values, " | ")) << std::endl;
}

void PrintValuesPretty(const dingodb::pb::meta::TableDefinition& table_definition, const std::vector<std::any>& values,
                       int32_t print_column_width, int64_t ts = 0) {  // NOLINT

  std::cout << "****************************************************************" << std::endl;
  if (ts > 0) {
    std::cout << fmt::format("{:>32}: {:<64}", "TS", ts) << std::endl;
  }
  for (int i = 0; i < values.size(); ++i) {
    const auto& column_definition = table_definition.columns().at(i);

    std::string str = dingodb::Helper::ConvertColumnValueToString(column_definition, values[i]);
    if (str.size() >= print_column_width) {
      str = str.substr(0, print_column_width - 3) + "...";
    }

    std::cout << fmt::format("{:>32}: {:<64}", column_definition.name(), str) << std::endl;
  }
}

std::string RecordToString(const dingodb::pb::meta::TableDefinition& table_definition,
                           const std::vector<std::any>& values) {
  std::vector<std::string> result;
  for (int i = 0; i < values.size(); ++i) {
    if (strcmp(values[i].type().name(), "v") == 0) {
      continue;
    }
    const auto& column_definition = table_definition.columns().at(i);

    result.push_back(dingodb::Helper::ConvertColumnValueToString(column_definition, values[i]));
  }

  return dingodb::Helper::VectorToString(result);
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

  auto serial_schema = dingodb::Utils::GenSerialSchema(table_definition);
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

    if (ctx->show_pretty) {
      PrintValuesPretty(table_definition, record, ctx->print_column_width);
    } else {
      PrintValues(table_definition, record, ctx->print_column_width);
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

  auto serial_schema = dingodb::Utils::GenSerialSchema(table_definition);
  auto record_encoder = std::make_shared<dingodb::RecordEncoder>(1, serial_schema, partition.id().entity_id());
  auto record_decoder = std::make_shared<dingodb::RecordDecoder>(1, serial_schema, partition.id().entity_id());

  std::string begin_key, end_key;
  record_encoder->EncodeMinKeyPrefix(dingodb::Constant::kExecutorTxn, begin_key);
  record_encoder->EncodeMaxKeyPrefix(dingodb::Constant::kExecutorTxn, end_key);
  std::string encode_begin_key = dingodb::mvcc::Codec::EncodeKey(begin_key, 0);
  std::string encode_end_key = dingodb::mvcc::Codec::EncodeKey(end_key, 0);

  auto lock_handler = [&](const std::string& key, const std::string& value) {
    std::string origin_key;
    int64_t ts;
    auto ret = dingodb::mvcc::Codec::DecodeKey(key, origin_key, ts);
    if (!ret) {
      LOG(INFO) << "decoce txn key failed, key: " << dingodb::Helper::StringToHex(key);
      return;
    }

    std::vector<std::any> record;
    ret = record_decoder->DecodeKey(origin_key, record);
    if (ret != 0) {
      LOG(INFO) << fmt::format("Decode failed, ret: {} record size: {}", ret, record.size());
    }

    dingodb::pb::store::LockInfo lock_info;
    if (!lock_info.ParseFromString(value)) {
      LOG(ERROR) << "parse pb string failed.";
      return;
    }

    std::cout << fmt::format("lock key({}) ts({}) value({})", RecordToString(table_definition, record), ts,
                             lock_info.ShortDebugString());
  };

  std::map<std::string, int64_t> last_datas;
  auto write_handler = [&](const std::string& key, const std::string& value) {
    std::string origin_key;
    int64_t ts;
    auto ret = dingodb::mvcc::Codec::DecodeKey(key, origin_key, ts);
    if (!ret) {
      LOG(INFO) << "decoce txn key failed, key: " << dingodb::Helper::StringToHex(key);
      return;
    }

    std::vector<std::any> record;
    ret = record_decoder->DecodeKey(origin_key, record);
    if (ret != 0) {
      LOG(INFO) << fmt::format("Decode failed, ret: {} record size: {}", ret, record.size());
    }

    dingodb::pb::store::WriteInfo write_info;
    if (!write_info.ParseFromString(value)) {
      LOG(ERROR) << "parse pb string failed.";
      return;
    }

    last_datas.insert(std::make_pair(origin_key, write_info.start_ts()));

    // parse short_value
    std::string decode_short_value;
    if (!write_info.short_value().empty()) {
      std::vector<std::any> record;
      int ret = record_decoder->Decode(origin_key, write_info.short_value(), record);
      if (ret != 0) {
        LOG(INFO) << fmt::format("Decode failed, ret: {} record size: {}", ret, record.size());
      }
      decode_short_value = RecordToString(table_definition, record);
    }

    std::cout << fmt::format("write key({}) ts({}) value({} {} {})", RecordToString(table_definition, record), ts,
                             write_info.start_ts(), dingodb::pb::store::Op_Name(write_info.op()), decode_short_value)
              << std::endl;
  };

  auto data_handler = [&](const std::string& key, const std::string& value) {
    std::string origin_key;
    int64_t ts;
    auto ret = dingodb::mvcc::Codec::DecodeKey(key, origin_key, ts);
    if (!ret) {
      LOG(INFO) << "decoce txn key failed, key: " << dingodb::Helper::StringToHex(key);
      return;
    }

    if (ctx->show_last_data) {
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
    ret = record_decoder->Decode(origin_key, value, record);
    if (ret != 0) {
      LOG(INFO) << fmt::format("Decode failed, ret: {} record size: {}", ret, record.size());
    }
    if (ctx->show_pretty) {
      PrintValuesPretty(table_definition, record, ts);
    } else {
      PrintValues(table_definition, record, ts);
    }
  };

  std::cout << fmt::format("table_id({}) partition_id({}) range[{}, {})", partition.id().parent_entity_id(),
                           partition.id().entity_id(), dingodb::Helper::StringToHex(begin_key),
                           dingodb::Helper::StringToHex(end_key))
            << std::endl;

  if (ctx->show_lock) {
    std::cout << fmt::format("=================== lock ====================") << std::endl;
    db->Scan(dingodb::Constant::kTxnLockCF, encode_begin_key, encode_end_key, ctx->offset, ctx->limit, lock_handler);
  }

  if (ctx->show_write) {
    std::cout << fmt::format("=================== write ====================") << std::endl;
    db->Scan(dingodb::Constant::kTxnWriteCF, encode_begin_key, encode_end_key, ctx->offset, ctx->limit, write_handler);
  }

  if (ctx->show_all_data || ctx->show_last_data) {
    std::cout << fmt::format("=================== data ====================") << std::endl;
    db->Scan(dingodb::Constant::kTxnDataCF, encode_begin_key, encode_end_key, ctx->offset, ctx->limit, data_handler);
  }
}

// void DumpExcutorTxn(std::shared_ptr<Context> ctx, dingodb::pb::meta::TableDefinition& table_definition,
//                     const dingodb::pb::meta::Partition& partition) {
//   auto db = std::make_shared<RocksDBOperator>(
//       ctx->db_path, std::vector<std::string>{dingodb::Constant::kStoreDataCF, dingodb::Constant::kTxnDataCF,
//                                              dingodb::Constant::kTxnLockCF, dingodb::Constant::kTxnWriteCF});
//   if (!db->Init()) {
//     return;
//   }

//   auto serial_schema = dingodb::Utils::GenSerialSchema(table_definition);
//   auto record_encoder = std::make_shared<dingodb::RecordEncoder>(1, serial_schema, partition.id().entity_id());
//   auto record_decoder = std::make_shared<dingodb::RecordDecoder>(1, serial_schema, partition.id().entity_id());

//   std::string begin_key, end_key;
//   record_encoder->EncodeMinKeyPrefix(dingodb::Constant::kExecutorTxn, begin_key);
//   record_encoder->EncodeMaxKeyPrefix(dingodb::Constant::kExecutorTxn, end_key);
//   begin_key = dingodb::Helper::EncodeTxnKey(begin_key, 0);
//   end_key = dingodb::Helper::EncodeTxnKey(end_key, 0);

//   auto lock_handler = [&](const std::string& key, const std::string& value) {
//     std::string origin_key;
//     int64_t ts;
//     auto status = dingodb::Helper::DecodeTxnKey(key, origin_key, ts);
//     if (!status.ok()) {
//       LOG(INFO) << "decoce txn key failed, error: " << status.error_str();
//       return;
//     }

//     std::vector<std::any> record;
//     int ret = record_decoder->DecodeKey(origin_key, record);
//     if (ret != 0) {
//       LOG(INFO) << fmt::format("Decode failed, ret: {} record size: {}", ret, record.size());
//     }

//     dingodb::pb::store::LockInfo lock_info;
//     if (!lock_info.ParseFromString(value)) {
//       LOG(ERROR) << "parse pb string failed.";
//       return;
//     }

//     std::cout << fmt::format("lock key({}) ts({}) value({})", RecordToString(table_definition, record), ts,
//                              lock_info.ShortDebugString());
//   };

//   std::map<std::string, int64_t> last_datas;
//   auto write_handler = [&](const std::string& key, const std::string& value) {
//     std::string origin_key;
//     int64_t ts;
//     auto status = dingodb::Helper::DecodeTxnKey(key, origin_key, ts);
//     if (!status.ok()) {
//       LOG(INFO) << "decoce txn key failed, error: " << status.error_str();
//       return;
//     }

//     std::vector<std::any> record;
//     int ret = record_decoder->DecodeKey(origin_key, record);
//     if (ret != 0) {
//       LOG(INFO) << fmt::format("Decode failed, ret: {} record size: {}", ret, record.size());
//     }

//     dingodb::pb::store::WriteInfo write_info;
//     if (!write_info.ParseFromString(value)) {
//       LOG(ERROR) << "parse pb string failed.";
//       return;
//     }

//     last_datas.insert(std::make_pair(origin_key, write_info.start_ts()));

//     // parse short_value
//     std::string decode_short_value;
//     if (!write_info.short_value().empty()) {
//       std::vector<std::any> record;
//       int ret = record_decoder->Decode(origin_key, write_info.short_value(), record);
//       if (ret != 0) {
//         LOG(INFO) << fmt::format("Decode failed, ret: {} record size: {}", ret, record.size());
//       }
//       decode_short_value = RecordToString(table_definition, record);
//     }

//     std::cout << fmt::format("write key({}) ts({}) value({} {} {})", RecordToString(table_definition, record), ts,
//                              write_info.start_ts(), dingodb::pb::store::Op_Name(write_info.op()), decode_short_value)
//               << std::endl;
//   };

//   auto data_handler = [&](const std::string& key, const std::string& value) {
//     std::string origin_key;
//     int64_t ts;
//     auto status = dingodb::Helper::DecodeTxnKey(key, origin_key, ts);
//     if (!status.ok()) {
//       LOG(INFO) << "decoce txn key failed, error: " << status.error_str();
//       return;
//     }

//     if (ctx->show_last_data) {
//       // filter not last ts data
//       auto it = last_datas.find(origin_key);
//       if (it != last_datas.end()) {
//         int64_t last_ts = it->second;
//         if (ts != last_ts) {
//           return;
//         }
//       }
//     }

//     std::vector<std::any> record;
//     int ret = record_decoder->Decode(origin_key, value, record);
//     if (ret != 0) {
//       LOG(INFO) << fmt::format("Decode failed, ret: {} record size: {}", ret, record.size());
//     }
//     if (ctx->show_pretty) {
//       PrintValuesPretty(table_definition, record, ctx->print_column_width, ts);
//     } else {
//       PrintValues(table_definition, record, ctx->print_column_width, ts);
//     }
//   };

//   std::cout << fmt::format("table_id({}) partition_id({}) range[{}, {})", partition.id().parent_entity_id(),
//                            partition.id().entity_id(), dingodb::Helper::StringToHex(begin_key),
//                            dingodb::Helper::StringToHex(end_key))
//             << std::endl;

//   if (ctx->show_lock) {
//     std::cout << fmt::format("=================== lock ====================") << std::endl;
//     db->Scan(dingodb::Constant::kTxnLockCF, begin_key, end_key, ctx->offset, ctx->limit, lock_handler);
//   }

//   if (ctx->show_write) {
//     std::cout << fmt::format("=================== write ====================") << std::endl;
//     db->Scan(dingodb::Constant::kTxnWriteCF, begin_key, end_key, ctx->offset, ctx->limit, write_handler);
//   }

//   if (ctx->show_all_data || ctx->show_last_data) {
//     std::cout << fmt::format("=================== data ====================") << std::endl;
//     db->Scan(dingodb::Constant::kTxnDataCF, begin_key, end_key, ctx->offset, ctx->limit, data_handler);
//   }
// }

void DumpClientRaw(std::shared_ptr<Context> ctx, dingodb::pb::meta::TableDefinition& table_definition,
                   const dingodb::pb::meta::Partition& partition) {
  auto db = std::make_shared<RocksDBOperator>(ctx->db_path, std::vector<std::string>{dingodb::Constant::kStoreDataCF});
  if (!db->Init()) {
    return;
  }

  // auto serial_schema = dingodb::Utils::GenSerialSchema(table_definition);
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

  auto encode_range = dingodb::mvcc::Codec::EncodeRange(partition.range());

  auto row_handler = [&](const std::string& key, const std::string& value) {
    LOG(INFO) << fmt::format("key: {} value: {}", dingodb::Helper::StringToHex(key),
                             dingodb::Helper::StringToHex(value));
  };

  db->Scan(dingodb::Constant::kTxnDataCF, encode_range.start_key(), encode_range.end_key(), ctx->offset, ctx->limit,
           row_handler);
}

void DumpVectorIndexRaw(std::shared_ptr<Context> ctx, dingodb::pb::meta::TableDefinition& table_definition) {
  auto vector_data_handler = [&](const std::string& key, const std::string& value) {
    if (ctx->show_vector) {
      dingodb::pb::common::Vector data;
      data.ParseFromString(value);
      int dimension = data.float_values_size() > 0 ? data.float_values_size() : data.binary_values_size();
      std::cout << fmt::format("[vector data] vector_id({}) value: dimension({}) {}",
                               dingodb::VectorCodec::UnPackageVectorId(key), dimension, FormatVector(data, 10))
                << std::endl;
    }
  };

  auto scalar_data_handler = [&](const std::string& key, const std::string& value) {
    if (ctx->show_vector) {
      dingodb::pb::common::VectorScalardata data;
      data.ParseFromString(value);
      std::cout << fmt::format("[scalar data] vector_id({}) value: {}", dingodb::VectorCodec::UnPackageVectorId(key),
                               data.ShortDebugString())
                << std::endl;
    }
  };

  auto table_data_handler = [&](const std::string& key, const std::string& value) {
    if (ctx->show_vector) {
      dingodb::pb::common::VectorTableData data;
      data.ParseFromString(value);
      std::cout << fmt::format("[table data] vector_id({}) table_key: {} table_value: {}",
                               dingodb::VectorCodec::UnPackageVectorId(key),
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

    char prefix = dingodb::Helper::GetKeyPrefix(partition.range().start_key());
    std::string begin_key = dingodb::VectorCodec::EncodeVectorKey(prefix, partition_id, 0);
    std::string end_key = dingodb::VectorCodec::EncodeVectorKey(prefix, partition_id, INT64_MAX);

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
                               dingodb::VectorCodec::UnPackageVectorId(key), dimension, FormatVector(data, 10))
                << std::endl;
    }
  };

  auto scalar_data_handler = [&](const std::string& key, const std::string& value) {
    if (ctx->show_vector) {
      dingodb::pb::common::VectorScalardata data;
      data.ParseFromString(value);
      std::cout << fmt::format("[scalar data] vector_id({}) value: {}", dingodb::VectorCodec::UnPackageVectorId(key),
                               data.ShortDebugString())
                << std::endl;
    }
  };

  auto table_data_handler = [&](const std::string& key, const std::string& value) {
    if (ctx->show_vector) {
      dingodb::pb::common::VectorTableData data;
      data.ParseFromString(value);
      std::cout << fmt::format("[table data] vector_id({}) table_key: {} table_value: {}",
                               dingodb::VectorCodec::UnPackageVectorId(key),
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

    char prefix = dingodb::Helper::GetKeyPrefix(partition.range().start_key());
    std::string begin_key = dingodb::VectorCodec::EncodeVectorKey(prefix, partition_id, 0);
    std::string end_key = dingodb::VectorCodec::EncodeVectorKey(prefix, partition_id, INT64_MAX);

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
      DumpClientTxn(ctx, table_definition, partition);

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

void WhichRegion(std::shared_ptr<Context> ctx) {
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

  if (table_definition.table_partition().strategy() == dingodb::pb::meta::PT_STRATEGY_HASH) {
    DINGO_LOG(ERROR) << "not support find hash partition type table/idnex.";
    return;
  }

  // get region range
  dingodb::pb::meta::IndexRange index_range;
  dingodb::pb::meta::TableRange table_range = SendGetTableRange(table_or_index_id);
  if (table_range.range_distribution().empty()) {
    index_range = SendGetIndexRange(table_or_index_id);
    if (index_range.range_distribution().empty()) {
      DINGO_LOG(ERROR) << "get table/index range failed.";
      return;
    }
  }

  auto range_distribution =
      !table_range.range_distribution().empty() ? table_range.range_distribution() : index_range.range_distribution();

  for (int i = range_distribution.size() - 1; i >= 0; --i) {
    const auto& distribution = range_distribution.at(i);
    int64_t partition_id = distribution.id().parent_entity_id();
    const auto& range = distribution.range();

    std::string encoded_key;
    if (table_definition.index_parameter().index_type() == dingodb::pb::common::INDEX_TYPE_NONE ||
        table_definition.index_parameter().index_type() == dingodb::pb::common::INDEX_TYPE_SCALAR) {
      auto serial_schema = dingodb::Utils::GenSerialSchema(table_definition);
      auto record_encoder = std::make_shared<dingodb::RecordEncoder>(1, serial_schema, partition_id);

      std::vector<std::string> origin_keys;
      dingodb::Helper::SplitString(ctx->key, ',', origin_keys);
      if (origin_keys.empty()) {
        DINGO_LOG(ERROR) << fmt::format("split key is empty");
        return;
      }
      record_encoder->EncodeKeyPrefix(dingodb::Helper::GetKeyPrefix(range.start_key()), origin_keys, encoded_key);

      if (encoded_key >= range.start_key()) {
        std::cout << fmt::format("Key locate region({}) range.", distribution.id().entity_id()) << std::endl;
        break;
      }

    } else if (table_definition.index_parameter().index_type() == dingodb::pb::common::INDEX_TYPE_VECTOR) {
      int64_t vector_id = dingodb::Helper::StringToInt64(ctx->key);

      dingodb::VectorCodec::EncodeVectorKey(dingodb::Helper::GetKeyPrefix(range.start_key()), partition_id, vector_id,
                                            encoded_key);

    } else if (table_definition.index_parameter().index_type() == dingodb::pb::common::INDEX_TYPE_DOCUMENT) {
      int64_t vector_id = dingodb::Helper::StringToInt64(ctx->key);

      dingodb::DocumentCodec::EncodeDocumentKey(dingodb::Helper::GetKeyPrefix(range.start_key()), partition_id,
                                                vector_id, encoded_key);
    }

    if (encoded_key >= range.start_key()) {
      std::cout << fmt::format("Key locate region({}) range.", distribution.id().entity_id()) << std::endl;
      break;
    }
  }
}

}  // namespace client_v2