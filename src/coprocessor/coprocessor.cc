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

#include "coprocessor/coprocessor.h"

#include <any>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <memory>
#include <utility>
#include <vector>

#include "common/logging.h"
#include "coprocessor/raw_coprocessor.h"
#include "coprocessor/utils.h"
#include "fmt/core.h"
#include "libexpr/src/expr/runner.h"
#include "proto/error.pb.h"
#include "proto/store.pb.h"
#include "rel_expr_helper.h"
#include "scan/scan_filter.h"
#include "serial/record_decoder.h"
#include "serial/record_encoder.h"

namespace dingodb {

bvar::Adder<uint64_t> Coprocessor::bvar_coprocessor_v1_object_running_num("dingo_coprocessor_v1_object_running_num");
bvar::Adder<uint64_t> Coprocessor::bvar_coprocessor_v1_object_total_num("dingo_coprocessor_v1_object_total_num");
bvar::LatencyRecorder Coprocessor::coprocessor_v1_latency("dingo_coprocessor_v1_latency");
bvar::Adder<uint64_t> Coprocessor::bvar_coprocessor_v1_execute_running_num("dingo_coprocessor_v1_execute_running_num");
bvar::Adder<uint64_t> Coprocessor::bvar_coprocessor_v1_execute_total_num("dingo_coprocessor_v1_execute_total_num");
bvar::LatencyRecorder Coprocessor::coprocessor_v1_execute_latency("dingo_coprocessor_v1_execute_latency");

Coprocessor::Coprocessor()
    : enable_expression_(true),
      end_of_group_by_(true),
      bvar_guard_for_coprocessor_v1_latency_(&coprocessor_v1_latency) {
  bvar_coprocessor_v1_object_running_num << 1;
  bvar_coprocessor_v1_object_total_num << 1;
}
Coprocessor::~Coprocessor() {
  Close();
  bvar_coprocessor_v1_object_running_num << -1;
}

butil::Status Coprocessor::Open(const std::any& coprocessor) {
  butil::Status status;

  DINGO_LOG(DEBUG) << fmt::format("Coprocessor::Open Enter");

  try {
    const CoprocessorPbWrapper& coprocessor_pb_wrapper = std::any_cast<const CoprocessorPbWrapper&>(coprocessor);

    const pb::store::Coprocessor* coprocessor_v1 = std::get_if<pb::store::Coprocessor>(&coprocessor_pb_wrapper);
    if (nullptr == coprocessor_v1) {
      std::string error_message =
          fmt::format("EXCEPTION from coprocessor_pb_wrapper trans pb::store::Coprocessor failed");
      DINGO_LOG(ERROR) << error_message;
      return butil::Status(pb::error::EILLEGAL_PARAMTETERS, error_message);
    }
    coprocessor_ = *coprocessor_v1;
  } catch (std::bad_any_cast& e) {
    std::string error_message = fmt::format("EXCEPTION : {} trans pb::store::Coprocessor failed", e.what());
    DINGO_LOG(ERROR) << error_message;
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, error_message);
  }

  Utils::DebugCoprocessor(coprocessor_);

  status = Utils::CheckPbSchema(coprocessor_.original_schema().schema());
  if (!status.ok()) {
    std::string error_message = fmt::format("original_schema check failed");
    DINGO_LOG(ERROR) << error_message;
    return status;
  }

  original_serial_schemas_ = std::make_shared<std::vector<std::shared_ptr<BaseSchema>>>();

  // init original_serial_schemas
  /**
   *   0    int      2
   *   1    string   1
   *   2    long     4
   *   3    double   5
   *   4    bool     6
   *   5    string   7
   *   6    long     0
   *   7    double   3
   */
  status = Utils::TransToSerialSchema(coprocessor_.original_schema().schema(), &original_serial_schemas_);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("TransToSerialSchema for original_serial_schemas  failed");
    return status;
  }
  Utils::DebugSerialSchema(original_serial_schemas_, "original_serial_schemas");

  // original_serial_schemas_sorted_ = std::make_shared<std::vector<std::shared_ptr<BaseSchema>>>();
  // Utils::CloneCloneSerialSchemaVector(original_serial_schemas_, &original_serial_schemas_sorted_);
  // sort by index
  // Utils::SortSerialSchemaVector(&original_serial_schemas_sorted_);

  // index from 0 ~ size-1
  // status = Utils::CheckSerialSchema(original_serial_schemas_sorted_);
  // if (!status.ok()) {
  //   std::string error_message = fmt::format("original_serial_schemas_sorted_ check failed");
  //   DINGO_LOG(ERROR) << error_message;
  //   return status;
  // }
  // Utils::DebugSerialSchema(original_serial_schemas_sorted_, "original_serial_schemas_sorted_");

  // status = Utils::CheckSelection(coprocessor_.selection_columns(), coprocessor_.original_schema().schema().size());
  // if (!status.ok()) {
  //   DINGO_LOG(ERROR) << fmt::format("selection_columns check failed");
  //   return status;
  // }

  // build selection column index
  // if (selection_column_indexes_.empty()) {
  //   GetSelectionColumnIndexes();
  // }
  GetOriginalColumnIndexes();
  /**
   *   6    int
   *   1    string
   *   0    long
   *   7    double
   *   2    bool
   *   3    double
   *   4    bool
   *   5    string
   */
  for (const auto& index : original_column_indexes_) {
    // Use the index variable here
    DINGO_LOG(DEBUG) << "original mapping column index:" << index;
  }

  GetSelectionColumnIndexes();

  for (const auto& index : selection_column_indexes_) {
    // Use the index variable here
    DINGO_LOG(DEBUG) << "select column index:" << index;
  }

  // selection_serial_schemas_ = std::make_shared<std::vector<std::shared_ptr<BaseSchema>>>();
  // Utils::CreateSerialSchema(original_serial_schemas_, coprocessor_.selection_columns(), &selection_serial_schemas_);
  // Utils::CreateSelectionSchema(original_serial_schemas_sorted_, selection_column_indexes_,
  // &selection_serial_schemas_);

  // Utils::DebugSerialSchema(selection_serial_schemas_, "selection_serial_schemas");
  // selection_serial_schemas_sorted_ = std::make_shared<std::vector<std::shared_ptr<BaseSchema>>>();
  // Utils::CloneCloneSerialSchemaVector(selection_serial_schemas_, &selection_serial_schemas_sorted_);
  // schema index start 0
  // Utils::UpdateSerialSchemaIndex(&selection_serial_schemas_sorted_); // index [0,2] ==> [0,1]
  // sort by index
  // Utils::SortSerialSchemaVector(&selection_serial_schemas_sorted_);
  // Utils::DebugSerialSchema(selection_serial_schemas_sorted_, "selection_serial_schemas_sorted_");

  status = InitGroupBySerialSchema(coprocessor_);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("InitGroupBySerialSchema failed");
    return status;
  }
  status = Utils::CheckPbSchema(coprocessor_.result_schema().schema());
  if (!status.ok()) {
    std::string error_message = fmt::format("result_schema check failed");
    DINGO_LOG(ERROR) << error_message;
    return status;
  }
  result_serial_schemas_ = std::make_shared<std::vector<std::shared_ptr<BaseSchema>>>();

  status = Utils::TransToSerialSchema(coprocessor_.result_schema().schema(), &result_serial_schemas_);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("TransToSerialSchema for result_serial_schemas failed");
    return status;
  }
  result_serial_schemas_sorted_ = std::make_shared<std::vector<std::shared_ptr<BaseSchema>>>();
  Utils::CloneCloneSerialSchemaVector(result_serial_schemas_, &result_serial_schemas_sorted_);
  // sort by index
  Utils::SortSerialSchemaVector(&result_serial_schemas_sorted_);

  // index from 0 ~ size-1
  status = Utils::CheckSerialSchema(result_serial_schemas_sorted_);
  if (!status.ok()) {
    std::string error_message = fmt::format("result_serial_schemas_sorted_ check failed");
    DINGO_LOG(ERROR) << error_message;
    return status;
  }

  status = CompareSerialSchema(coprocessor_);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("CompareSerialSchema failed");
    return status;
  }
  enable_expression_ = !coprocessor_.expression().empty();

  DINGO_LOG(DEBUG) << fmt::format("Coprocessor::Open enable_expression_ : {}", enable_expression_);

  DINGO_LOG(DEBUG) << fmt::format("Coprocessor::Open Leave");

  // Utils::DebugSerialSchema(original_serial_schemas_, "original_serial_schemas");
  // Utils::DebugSerialSchema(original_serial_schemas_sorted_, "original_serial_schemas_sorted_");
  // Utils::DebugSerialSchema(group_by_key_serial_schemas_, "group_by_key_serial_schemas");
  // Utils::DebugSerialSchema(group_by_operator_serial_schemas_, "group_by_operator_serial_schemas");
  // Utils::DebugSerialSchema(group_by_serial_schemas_, "group_by_serial_schemas");
  Utils::DebugSerialSchema(result_serial_schemas_, "result_serial_schemas");
  Utils::DebugSerialSchema(result_serial_schemas_sorted_, "result_serial_schemas_sorted_");

  return butil::Status();
}

butil::Status Coprocessor::Execute(IteratorPtr iter, bool key_only, size_t max_fetch_cnt, int64_t max_bytes_rpc,
                                   std::vector<pb::common::KeyValue>* kvs, bool& /*has_more*/) {
  BvarLatencyGuard bvar_guard(&coprocessor_v1_execute_latency);
  Coprocessor::bvar_coprocessor_v1_execute_running_num << 1;
  Coprocessor::bvar_coprocessor_v1_execute_total_num << 1;
  ON_SCOPE_EXIT([&]() { Coprocessor::bvar_coprocessor_v1_execute_running_num << -1; });
  DINGO_LOG(DEBUG) << fmt::format("Coprocessor::Execute Enter");
  ScanFilter scan_filter = ScanFilter(key_only, max_fetch_cnt, max_bytes_rpc);
  butil::Status status;
  while (iter->Valid()) {
    pb::common::KeyValue kv;
    *kv.mutable_key() = iter->Key();
    *kv.mutable_value() = iter->Value();
    bool has_result_kv = false;
    pb::common::KeyValue result_key_value;
    DINGO_LOG(DEBUG) << fmt::format("Coprocessor::DoExecute Call");
    status = DoExecute(kv, &has_result_kv, &result_key_value);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << fmt::format("Coprocessor::Execute failed");
      return status;
    }

    if (!has_result_kv) {
      iter->Next();
      continue;
    }

    if (key_only) {
      result_key_value.set_value("");
    }

    kvs->emplace_back(result_key_value);

    if (scan_filter.UptoLimit(result_key_value)) {
      iter->Next();
      return butil::Status();
    }
    iter->Next();
  }

  status = GetKeyValueFromAggregation(key_only, max_fetch_cnt, max_bytes_rpc, kvs);

  DINGO_LOG(DEBUG) << fmt::format("Coprocessor::Execute Leave");

  return status;
}

butil::Status Coprocessor::Execute(TxnIteratorPtr iter, int64_t limit, bool key_only, bool is_reverse,
                                   pb::store::TxnResultInfo& txn_result_info, std::vector<pb::common::KeyValue>& kvs,
                                   bool& has_more, std::string& end_key) {
  return RawCoprocessor::Execute(iter, limit, key_only, is_reverse, txn_result_info, kvs, has_more, end_key);
}

butil::Status Coprocessor::Filter(const std::string& key, const std::string& value, bool& is_reserved) {
  return RawCoprocessor::Filter(key, value, is_reserved);
}

butil::Status Coprocessor::Filter(const pb::common::VectorScalardata& scalar_data, bool& is_reserved) {
  return RawCoprocessor::Filter(scalar_data, is_reserved);
}

butil::Status Coprocessor::DoExecute(const pb::common::KeyValue& kv, bool* has_result_kv,
                                     pb::common::KeyValue* result_kv) {
  butil::Status status;

  RecordDecoder original_record_decoder(coprocessor_.schema_version(), original_serial_schemas_,
                                        coprocessor_.original_schema().common_id());

  std::vector<std::any> original_record;

  // if (original_column_indexes_.empty()) {
  //   GetOriginalColumnIndexes();
  // }
  // if(selection_column_indexes_.empty()) {
  //   GetSelectionColumnIndexes();
  // }

  // for (const auto& index : selection_column_indexes_) {
  //   // Use the index variable here
  //   DINGO_LOG_DEBUG << "selection column index:" << index;
  // }

  int ret = 0;
  try {
    // decode some column. not decode all
    ret = original_record_decoder.Decode(kv, selection_column_indexes_, original_record);
  } catch (const std::exception& my_exception) {
    std::string error_message = fmt::format("serial::Decode failed exception : {}", my_exception.what());
    DINGO_LOG(ERROR) << error_message;
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, error_message);
  }

  if (ret < 0) {
    std::string error_message = fmt::format("serial::Decode failed");
    DINGO_LOG(ERROR) << error_message;
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, error_message);
  }

  //  std::vector<std::any> selection_record;
  //  selection_record.reserve(coprocessor_.selection_columns().size());
  //  size_t i = 0;
  //  for (auto index : coprocessor_.selection_columns()) {
  //    std::any column = Utils::CloneColumn(original_record[index], (*selection_serial_schemas_sorted_)[i]->GetType());
  //    if (!column.has_value()) {
  //      std::string error_message = fmt::format(
  //          "CloneColumn failed original_record index : {} selection_serial_schemas_sorted_ i : {} "
  //          "selection_serial_schemas_sorted_ "
  //          "type : {}",
  //          index, i, static_cast<int>((*selection_serial_schemas_sorted_)[i]->GetType()));
  //      DINGO_LOG(ERROR) << error_message;
  //      return butil::Status(pb::error::EILLEGAL_PARAMTETERS, error_message);
  //    }
  //    selection_record.emplace_back(std::move(column));
  //    i++;
  //  }

  bool is_key_value_reserve = true;
  if (enable_expression_) {
    expr::Runner runner;

    try {
      runner.Decode(reinterpret_cast<const expr::Byte*>(coprocessor_.expression().c_str()),
                    coprocessor_.expression().length());
      auto tuple = std::make_unique<expr::Tuple>();
      RelExprHelper::TransToOperandWrapper(original_serial_schemas_, selection_column_indexes_, original_record, tuple);
      runner.BindTuple(tuple.get());
      runner.Run();
      std::optional<bool> ok = runner.GetOptional<bool>();
      is_key_value_reserve = ok.has_value() && ok.value();
    } catch (const std::exception& my_exception) {
      std::string error_message = fmt::format("expr::Runner Decode or Run failed. exception : {}", my_exception.what());
      DINGO_LOG(ERROR) << error_message;
      return butil::Status(pb::error::EILLEGAL_PARAMTETERS, error_message);
    }
  }

  // discard this key value
  if (!is_key_value_reserve) {
    return butil::Status();
  }

  if (end_of_group_by_) {  // group by
    status = DoExecuteForAggregation(original_record);
    if (!status.ok()) {
      std::string error_message = fmt::format("Coprocessor::DoExecuteForAggregation failed");
      DINGO_LOG(ERROR) << error_message;
      return status;
    }

    *has_result_kv = false;

  } else {  // selection
    status = DoExecuteForSelection(original_record, has_result_kv, result_kv);
    if (!status.ok()) {
      std::string error_message = fmt::format("Coprocessor::DoExecuteForSelection failed");
      DINGO_LOG(ERROR) << error_message;
      return status;
    }
  }

  return butil::Status();
}

butil::Status Coprocessor::DoExecuteForAggregation(const std::vector<std::any>& selection_record) {
  butil::Status status;
  // group by
  std::vector<std::any> group_by_key_record;
  std::vector<std::any> group_by_operator_record;

  group_by_key_record.reserve(coprocessor_.group_by_columns_size());
  group_by_operator_record.reserve(coprocessor_.aggregation_operators_size());

  {
    size_t i = 0;
    for (auto index : coprocessor_.group_by_columns()) {
      std::any column = Utils::CloneColumn(selection_record[index], (*group_by_key_serial_schemas_)[i]->GetType());
      if (!column.has_value()) {
        std::string error_message = fmt::format(
            "CloneColumn failed selection_record index : {} group_by_key_serial_schemas_ i : {} "
            "group_by_key_serial_schemas_ type : {}",
            index, i, static_cast<int>((*group_by_key_serial_schemas_)[i]->GetType()));
        DINGO_LOG(ERROR) << error_message;
        return butil::Status(pb::error::EILLEGAL_PARAMTETERS, error_message);
      }
      // debug
      Utils::DebugColumn(column, (*group_by_key_serial_schemas_)[i]->GetType(), "key");

      group_by_key_record.emplace_back(std::move(column));
      i++;
    }
  }

  {
    size_t i = 0;
    for (const auto& aggregation : coprocessor_.aggregation_operators()) {
      int32_t index_of_column =
          (aggregation.index_of_column() < 0 || aggregation.index_of_column() >= selection_column_indexes_.size())
              ? 0
              : aggregation.index_of_column();
      std::any column =
          Utils::CloneColumn(selection_record[index_of_column], (*group_by_operator_serial_schemas_)[i]->GetType());
      if (!column.has_value()) {
        std::string error_message = fmt::format(
            "CloneColumn failed selection_record index_of_column : {}  group_by_operator_serial_schemas_ i : {}  "
            "group_by_operator_serial_schemas_ type : {}",
            index_of_column, i, static_cast<int>((*group_by_operator_serial_schemas_)[i]->GetType()));
        DINGO_LOG(ERROR) << error_message;
        return butil::Status(pb::error::EILLEGAL_PARAMTETERS, error_message);
      }
      // debug
      Utils::DebugColumn(column, (*group_by_operator_serial_schemas_)[i]->GetType(), "aggregation_operators");
      group_by_operator_record.emplace_back(std::move(column));
      i++;
    }
  }

  std::string group_by_key;
  if (group_by_key_serial_schemas_ && !group_by_key_serial_schemas_->empty()) {
    RecordEncoder group_by_key_encoder(coprocessor_.schema_version(), group_by_key_serial_schemas_,
                                       coprocessor_.result_schema().common_id());
    int ret = 0;
    try {
      // group_by_key_record [0,1,2,3,4,5,6] sort, for group_by_key_serial_schemas_ in vector index no schema index
      ret = group_by_key_encoder.EncodeKey(group_by_key_record, group_by_key);
    } catch (const std::exception& my_exception) {
      std::string error_message = fmt::format("serial::EncodeKey failed exception : {}", my_exception.what());
      DINGO_LOG(ERROR) << error_message;
      return butil::Status(pb::error::EILLEGAL_PARAMTETERS, error_message);
    }
    if (ret < 0) {
      std::string error_message = fmt::format("serial::EncodeKey failed");
      DINGO_LOG(ERROR) << error_message;
      return butil::Status(pb::error::EILLEGAL_PARAMTETERS, error_message);
    }
  }

  Utils::DebugGroupByKey(group_by_key, "group_by_key");

  if (!aggregation_manager_) {
    aggregation_manager_ = std::make_shared<AggregationManager>();
    status = aggregation_manager_->Open(group_by_operator_serial_schemas_, coprocessor_.aggregation_operators(),
                                        result_serial_schemas_);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << fmt::format("AggregationManager::Open failed");
      return status;
    }
  }

  status = aggregation_manager_->Execute(group_by_key, group_by_operator_record);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("AggregationManager::Execute failed");
    return status;
  }
  return butil::Status();
}

butil::Status Coprocessor::DoExecuteForSelection(const std::vector<std::any>& selection_record, bool* has_result_kv,
                                                 pb::common::KeyValue* result_kv) {
  butil::Status status;
  // selection
  RecordEncoder result_record_encoder(coprocessor_.schema_version(), result_serial_schemas_sorted_,
                                      coprocessor_.result_schema().common_id());
  pb::common::KeyValue result_key_value;
  int ret = 0;
  try {
    ret = result_record_encoder.Encode(selection_record, result_key_value);
  } catch (const std::exception& my_exception) {
    std::string error_message = fmt::format("serial::Encode failed exception : {}", my_exception.what());
    DINGO_LOG(ERROR) << error_message;
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, error_message);
  }
  if (ret < 0) {
    std::string error_message = fmt::format("serial::Encode failed");
    DINGO_LOG(ERROR) << error_message;
    return status;
  }

  *has_result_kv = true;
  *result_kv = std::move(result_key_value);

  return butil::Status();
}

butil::Status Coprocessor::GetKeyValueFromAggregation(bool key_only, size_t max_fetch_cnt, int64_t max_bytes_rpc,
                                                      std::vector<pb::common::KeyValue>* kvs) {
  butil::Status status;

  if (end_of_group_by_ && aggregation_manager_) {
    if (!aggregation_iterator_) {
      aggregation_iterator_ = aggregation_manager_->CreateIterator();
    }
    ScanFilter scan_filter = ScanFilter(key_only, max_fetch_cnt, max_bytes_rpc);

    RecordEncoder result_record_encoder(coprocessor_.schema_version(), result_serial_schemas_,
                                        coprocessor_.result_schema().common_id());

    while (aggregation_iterator_->HasNext()) {
      Utils::DebugGroupByKey("", "Key Value pair");
      const std::string& key = aggregation_iterator_->GetKey();
      const std::shared_ptr<std::vector<std::any>>& value = aggregation_iterator_->GetValue();

      std::vector<std::any> result_key_record;
      int ret = 0;
      if (group_by_key_serial_schemas_ && !group_by_key_serial_schemas_->empty()) {
        RecordDecoder result_record_decoder(coprocessor_.schema_version(), group_by_key_serial_schemas_,
                                            coprocessor_.result_schema().common_id());

        try {
          ret = result_record_decoder.DecodeKey(key, result_key_record);
        } catch (const std::exception& my_exception) {
          std::string error_message = fmt::format("serial::DecodeKey failed exception : {}", my_exception.what());
          DINGO_LOG(ERROR) << error_message;
          return butil::Status(pb::error::EILLEGAL_PARAMTETERS, error_message);
        }
        if (ret < 0) {
          std::string error_message = fmt::format("serial::DecodeKey failed");
          DINGO_LOG(ERROR) << error_message;
          return butil::Status(pb::error::EILLEGAL_PARAMTETERS, error_message);
        }
      }

      std::vector<std::any> result_record;
      result_record.reserve(result_key_record.size() + value->size());
      size_t i = 0;
      for (const auto& column : result_key_record) {
        std::any column_clone = Utils::CloneColumn(column, (*result_serial_schemas_sorted_)[i]->GetType());
        if (!column_clone.has_value()) {
          std::string error_message = fmt::format(
              "CloneColumn failed result_key_record index : {} result_serial_schemas_sorted_ i : {} "
              "result_serial_schemas_sorted_ "
              "type : {}",
              i, i, BaseSchema::GetTypeString((*result_serial_schemas_sorted_)[i]->GetType()));
          DINGO_LOG(ERROR) << error_message;
          return butil::Status(pb::error::EILLEGAL_PARAMTETERS, error_message);
        }
        Utils::DebugColumn(column, (*result_serial_schemas_sorted_)[i]->GetType(), "Key");
        result_record.emplace_back(std::move(column_clone));
        i++;
      }

      for (const auto& column : *value) {
        std::any column_clone = Utils::CloneColumn(column, (*result_serial_schemas_sorted_)[i]->GetType());
        if (!column_clone.has_value()) {
          std::string error_message = fmt::format(
              "CloneColumn failed result_aggregation_record  index : {} result_serial_schemas_sorted_ i : {} "
              "result_serial_schemas_sorted_ type : {}",
              (i - result_key_record.size()), i,
              BaseSchema::GetTypeString((*result_serial_schemas_sorted_)[i]->GetType()));
          DINGO_LOG(ERROR) << error_message;
          return butil::Status(pb::error::EILLEGAL_PARAMTETERS, error_message);
        }
        Utils::DebugColumn(column, (*result_serial_schemas_sorted_)[i]->GetType(), "Value");
        result_record.emplace_back(std::move(column_clone));
        i++;
      }

      pb::common::KeyValue result_key_value;
      ret = 0;
      try {
        ret = result_record_encoder.Encode(result_record, result_key_value);
      } catch (const std::exception& my_exception) {
        std::string error_message = fmt::format("serial::Encode failed exception : {}", my_exception.what());
        DINGO_LOG(ERROR) << error_message;
        return butil::Status(pb::error::EILLEGAL_PARAMTETERS, error_message);
      }
      if (ret < 0) {
        std::string error_message = fmt::format("serial::Encode failed");
        DINGO_LOG(ERROR) << error_message;
        return butil::Status(pb::error::EILLEGAL_PARAMTETERS, error_message);
      }

      if (key_only) {
        result_key_value.set_value("");
      }

      kvs->emplace_back(result_key_value);

      if (scan_filter.UptoLimit(result_key_value)) {
        aggregation_iterator_->Next();
        return butil::Status();
      }

      aggregation_iterator_->Next();
    }
  }

  return butil::Status();
}

void Coprocessor::Close() {
  coprocessor_.Clear();
  if (original_serial_schemas_) {
    original_serial_schemas_->clear();
  }

  if (selection_serial_schemas_) {
    selection_serial_schemas_->clear();
  }

  if (group_by_key_serial_schemas_) {
    group_by_key_serial_schemas_->clear();
  }

  if (group_by_operator_serial_schemas_) {
    group_by_operator_serial_schemas_->clear();
  }

  if (group_by_serial_schemas_) {
    group_by_serial_schemas_->clear();
  }

  if (result_serial_schemas_) {
    result_serial_schemas_->clear();
  }

  enable_expression_ = false;
  end_of_group_by_ = false;

  if (aggregation_manager_) {
    aggregation_manager_.reset();
  }

  if (aggregation_iterator_) {
    aggregation_iterator_.reset();
  }

  original_column_indexes_.clear();
  selection_column_indexes_.clear();

  if (original_serial_schemas_sorted_) {
    original_serial_schemas_sorted_.reset();
  }

  if (selection_serial_schemas_sorted_) {
    selection_serial_schemas_sorted_.reset();
  }

  if (result_serial_schemas_sorted_) {
    result_serial_schemas_sorted_.reset();
  }
}

butil::Status Coprocessor::CompareSerialSchema(const pb::store::Coprocessor& coprocessor) {
  butil::Status status;

  // group by
  // DINGO_LOG_DEBUG << "result_serial_schemas_sorted_->size():" << result_serial_schemas_sorted_->size();
  if (group_by_serial_schemas_ && !group_by_serial_schemas_->empty()) {
    DINGO_LOG_DEBUG << "group_by_serial_schemas_->size():" << group_by_serial_schemas_->size();
    if (coprocessor.result_schema().schema().size() != group_by_serial_schemas_->size()) {
      std::string error_message =
          fmt::format("enable group by result_serial_schemas_sorted_ : {} unequal group_by_serial_schemas_ : {}",
                      result_serial_schemas_sorted_->size(), group_by_serial_schemas_->size());
      DINGO_LOG(ERROR) << error_message;
      return butil::Status(pb::error::EILLEGAL_PARAMTETERS, error_message);
    }

    // status = Utils::CompareSerialSchemaNonStrict(result_serial_schemas_sorted_, group_by_serial_schemas_,
    //                                              coprocessor.group_by_columns(),
    //                                              coprocessor.aggregation_operators());
    // if (!status.ok()) {
    //   std::string error_message = fmt::format(
    //       "CompareSerialSchemaNonStrict failed. compare result_serial_schemas_sorted_ and  selection_serial_schemas_
    //       ");
    //   DINGO_LOG(ERROR) << error_message;
    //   return status;
    // }

    end_of_group_by_ = true;
    DINGO_LOG(DEBUG) << fmt::format("Coprocessor::Open enable group_by");

  } else {
    end_of_group_by_ = false;
    DINGO_LOG(DEBUG) << fmt::format("Coprocessor::Open enable selection");
  }

  return butil::Status();
}

butil::Status Coprocessor::InitGroupBySerialSchema(const pb::store::Coprocessor& coprocessor) {
  butil::Status status;
  status = Utils::CheckGroupByColumns(coprocessor.group_by_columns(), selection_column_indexes_.size());
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("group by columns check failed");
    return status;
  }

  if (!coprocessor.group_by_columns().empty()) {
    group_by_key_serial_schemas_ = std::make_shared<std::vector<std::shared_ptr<BaseSchema>>>();

    // Utils::CreateSerialSchema(selection_serial_schemas_sorted_, coprocessor.group_by_columns(),
    //                           &group_by_key_serial_schemas_);

    Utils::CreateSerialSchema(original_serial_schemas_, coprocessor.group_by_columns(), selection_column_indexes_,
                              &group_by_key_serial_schemas_);

    // Utils::UpdateSerialSchemaIndex(&group_by_key_serial_schemas_); // [1,2] ==> [0,1]

    std::vector<bool> keys;
    keys.resize(coprocessor.group_by_columns().size(), true);

    Utils::UpdateSerialSchemaKey(keys, &group_by_key_serial_schemas_);

    DINGO_LOG(DEBUG) << fmt::format("Coprocessor::Open enable group_by_key");
  }

  status = Utils::CheckGroupByOperators(coprocessor.aggregation_operators(), selection_column_indexes_.size());
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("group by operators check failed");
    return status;
  }

  if (!coprocessor.aggregation_operators().empty()) {
    group_by_operator_serial_schemas_ = std::make_shared<std::vector<std::shared_ptr<BaseSchema>>>();
    ::google::protobuf::RepeatedField<int32_t> aggregation_operator_columns;
    for (const auto& aggregation_operator : coprocessor.aggregation_operators()) {
      DINGO_LOG(DEBUG) << aggregation_operator.index_of_column();
      aggregation_operator_columns.Add(aggregation_operator.index_of_column() < 0 ||
                                               aggregation_operator.index_of_column() >=
                                                   selection_column_indexes_.size()
                                           ? 0
                                           : aggregation_operator.index_of_column());
    }
    DINGO_LOG(DEBUG) << "CreateSerialSchema########";
    Utils::CreateSerialSchema(original_serial_schemas_, aggregation_operator_columns, selection_column_indexes_,
                              &group_by_operator_serial_schemas_);
    DINGO_LOG(DEBUG) << "CreateSerialSchema######## end";
    // Utils::UpdateSerialSchemaIndex(&group_by_operator_serial_schemas_);

    std::vector<bool> keys;
    keys.resize(coprocessor.group_by_columns().size(), false);

    Utils::UpdateSerialSchemaKey(keys, &group_by_operator_serial_schemas_);

    DINGO_LOG(DEBUG) << fmt::format("Coprocessor::Open enable group_by_operator");
  }

  Utils::DebugSerialSchema(group_by_key_serial_schemas_, "group_by_key_serial_schemas");
  Utils::DebugSerialSchema(group_by_operator_serial_schemas_, "group_by_operator_serial_schemas");

  // complete Sum(a) group by b
  if (!coprocessor.group_by_columns().empty() || !coprocessor.aggregation_operators().empty()) {
    Utils::JoinSerialSchema(group_by_key_serial_schemas_, group_by_operator_serial_schemas_, &group_by_serial_schemas_);
    Utils::DebugSerialSchema(group_by_serial_schemas_, "group_by_serial_schemas");
    Utils::UpdateSerialSchemaIndex(&group_by_serial_schemas_);
    Utils::DebugSerialSchema(group_by_serial_schemas_, "group_by_serial_schemas");
  }
  return butil::Status();
}

void Coprocessor::GetOriginalColumnIndexes() {
  original_column_indexes_.resize(original_serial_schemas_->size(), -1);
  int i = 0;
  for (const auto& schema : *original_serial_schemas_) {
    int index = schema->GetIndex();
    DINGO_LOG(DEBUG) << index << "," << i;
    original_column_indexes_[index] = i;
    i++;
  }

  // sort and unique
  // std::sort(original_column_indexes_.begin(), original_column_indexes_.end(), [](int i, int j) { return i < j; });

  // original_column_indexes_.erase(std::unique(original_column_indexes_.begin(), original_column_indexes_.end()),
  //                                original_column_indexes_.end());
}

void Coprocessor::GetSelectionColumnIndexes() {
  if (coprocessor_.selection_columns().empty()) {
    DINGO_LOG(DEBUG) << "empty()";
    // selection_column_indexes_.resize(original_column_indexes_.size(), -1);
    for (const auto& index : original_column_indexes_) {
      int i = index;
      DINGO_LOG(DEBUG) << "i:" << i;
      selection_column_indexes_.push_back(i);
    }
    // sort and unique
    // std::sort(selection_column_indexes_.begin(), selection_column_indexes_.end(), [](int i, int j) { return i < j;
    // });

    // selection_column_indexes_.erase(std::unique(selection_column_indexes_.begin(), selection_column_indexes_.end()),
    // selection_column_indexes_.end());
  } else {
    // [0,2,4,6]  => [6,0,2,4]
    // selection_column_indexes_.resize(coprocessor_.selection_columns().size(), -1);
    for (const auto& index : coprocessor_.selection_columns()) {
      int i = index;
      DINGO_LOG(DEBUG) << "index:" << i;
      selection_column_indexes_.push_back(original_column_indexes_[i]);
    }
    // sort and unique
    // std::sort(selection_column_indexes_.begin(), selection_column_indexes_.end(), [](int i, int j) { return i < j;
    // });

    // selection_column_indexes_.erase(std::unique(selection_column_indexes_.begin(), selection_column_indexes_.end()),
    //                                 selection_column_indexes_.end());
  }
}

}  // namespace dingodb
