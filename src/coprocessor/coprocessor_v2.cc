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

#include "coprocessor/coprocessor_v2.h"

#include <algorithm>
#include <any>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <limits>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "common/logging.h"
#include "coprocessor/utils.h"
#include "engine/txn_engine_helper.h"
#include "fmt/core.h"
#include "glog/logging.h"
#include "proto/error.pb.h"
#include "proto/store.pb.h"
#include "scan/scan_filter.h"
#include "serial/record_decoder.h"
#include "serial/record_encoder.h"

namespace dingodb {

DECLARE_int64(max_scan_memory_size);
DECLARE_int64(max_scan_line_limit);

CoprocessorV2::CoprocessorV2() = default;
CoprocessorV2::~CoprocessorV2() { Close(); }

butil::Status CoprocessorV2::Open(const std::any& coprocessor) {
  butil::Status status;

  DINGO_LOG(DEBUG) << fmt::format("CoprocessorV2::Open Enter");

  try {
    const CoprocessorPbWrapper& coprocessor_pb_wrapper = std::any_cast<const CoprocessorPbWrapper&>(coprocessor);

    const pb::common::CoprocessorV2* coprocessor_v2 = std::get_if<pb::common::CoprocessorV2>(&coprocessor_pb_wrapper);
    if (nullptr == coprocessor_v2) {
      std::string error_message =
          fmt::format("EXCEPTION from coprocessor_pb_wrapper trans pb::common::CoprocessorV2 failed");
      DINGO_LOG(ERROR) << error_message;
      return butil::Status(pb::error::EILLEGAL_PARAMTETERS, error_message);
    }
    coprocessor_ = *coprocessor_v2;
  } catch (std::bad_any_cast& e) {
    std::string error_message = fmt::format("EXCEPTION : {} trans pb::common::CoprocessorV2 failed", e.what());
    DINGO_LOG(ERROR) << error_message;
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, error_message);
  }

  Utils::DebugCoprocessorV2(coprocessor_);

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

  GetOriginalColumnIndexes();
  ShowOriginalColumnIndexes();

  GetSelectionColumnIndexes();
  ShowSelectionColumnIndexes();

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

  Utils::DebugSerialSchema(result_serial_schemas_, "result_serial_schemas");

  GetResultColumnIndexes();
  ShowResultColumnIndexes();

  if (coprocessor_.rel_expr().empty()) {
    std::string error_message = fmt::format("CoprocessorV2::Open rel_expr empty. not support");
    DINGO_LOG(ERROR) << error_message;
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, error_message);
  }

  original_record_decoder_ = std::make_shared<RecordDecoder>(coprocessor_.schema_version(), original_serial_schemas_,
                                                             coprocessor_.original_schema().common_id());

  result_record_encoder_ = std::make_shared<RecordEncoder>(coprocessor_.schema_version(), result_serial_schemas_,
                                                           coprocessor_.result_schema().common_id());

  rel_runner_ = std::make_shared<expr::RelRunner>();

  try {
    rel_runner_->Decode(reinterpret_cast<const expr::Byte*>(coprocessor_.rel_expr().c_str()),
                        coprocessor_.rel_expr().length());
  } catch (const std::exception& my_exception) {
    std::string error_message = fmt::format("expr::RelRunner Decode failed. exception : {}", my_exception.what());
    DINGO_LOG(ERROR) << error_message;
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, error_message);
  }

  return status;
}

butil::Status CoprocessorV2::Execute(IteratorPtr iter, bool key_only, size_t max_fetch_cnt, int64_t max_bytes_rpc,
                                     std::vector<pb::common::KeyValue>* kvs) {
  DINGO_LOG(DEBUG) << fmt::format("CoprocessorV2::Execute IteratorPtr Enter");
  ScanFilter scan_filter = ScanFilter(false, max_fetch_cnt, max_bytes_rpc);
  butil::Status status;

  while (iter->Valid()) {
    pb::common::KeyValue kv;
    *kv.mutable_key() = iter->Key();
    *kv.mutable_value() = iter->Value();
    bool has_result_kv = false;
    pb::common::KeyValue result_key_value;
    DINGO_LOG(DEBUG) << fmt::format("CoprocessorV2::DoExecute Call");
    status = DoExecute(kv.key(), kv.value(), &has_result_kv, &result_key_value);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << fmt::format("CoprocessorV2::Execute failed");
      return status;
    }

    if (has_result_kv) {
      if (key_only) {
        result_key_value.set_value("");
      }

      kvs->emplace_back(std::move(result_key_value));
    }

    if (scan_filter.UptoLimit(kv)) {
      DINGO_LOG(WARNING) << fmt::format(
          "CoprocessorV2 UptoLimit. key_only : {} max_fetch_cnt ; [} max_bytes_rpc : {} cur_fetch_cnt : {} "
          "cur_bytes_rpc : {}",
          key_only, max_fetch_cnt, max_bytes_rpc, scan_filter.GetCurFetchCnt(), scan_filter.GetCurBytesRpc());
      iter->Next();
      break;
    }

    iter->Next();
  }

  status = GetKvFromExprEndOfFinish(key_only, max_fetch_cnt, max_bytes_rpc, kvs);

  DINGO_LOG(DEBUG) << fmt::format("CoprocessorV2::Execute IteratorPtr Leave");

  return status;
}

butil::Status CoprocessorV2::Execute(TxnIteratorPtr iter, int64_t limit, bool key_only, bool /*is_reverse*/,
                                     pb::store::TxnResultInfo& txn_result_info, std::vector<pb::common::KeyValue>& kvs,
                                     bool& has_more, std::string& end_key) {
  DINGO_LOG(DEBUG) << fmt::format("CoprocessorV2::Execute  TxnIteratorPtr Enter");

  butil::Status status;

  ScanFilter scan_filter =
      ScanFilter(false, std::min(limit, FLAGS_max_scan_line_limit), std::numeric_limits<int64_t>::max());

  while (iter->Valid(txn_result_info)) {
    pb::common::KeyValue kv;
    *kv.mutable_key() = iter->Key();
    *kv.mutable_value() = iter->Value();
    bool has_result_kv = false;
    pb::common::KeyValue result_key_value;
    DINGO_LOG(DEBUG) << fmt::format("CoprocessorV2::DoExecute Call");
    status = DoExecute(kv.key(), kv.value(), &has_result_kv, &result_key_value);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << fmt::format("CoprocessorV2::Execute failed");
      return status;
    }

    end_key = iter->Key();

    if (has_result_kv) {
      if (key_only) {
        result_key_value.set_value("");
      }

      kvs.emplace_back(std::move(result_key_value));
    }

    if (scan_filter.UptoLimit(kv)) {
      has_more = true;
      DINGO_LOG(WARNING) << fmt::format(
          "CoprocessorV2 UptoLimit. key_only : {} max_fetch_cnt ; [} max_bytes_rpc : {} cur_fetch_cnt : {} "
          "cur_bytes_rpc : {}",
          key_only, std::min(limit, FLAGS_max_scan_line_limit), std::numeric_limits<int64_t>::max(),
          scan_filter.GetCurFetchCnt(), scan_filter.GetCurBytesRpc());
      iter->Next();
      break;
    }

    iter->Next();
  }

  status = GetKvFromExprEndOfFinish(key_only, limit, FLAGS_max_scan_memory_size, &kvs);

  DINGO_LOG(DEBUG) << fmt::format("CoprocessorV2::Execute TxnIteratorPtr Leave");

  return status;
}

butil::Status CoprocessorV2::Filter(const std::string& key, const std::string& value, bool& is_reserved) {
  return DoFilter(key, value, &is_reserved);
}

butil::Status CoprocessorV2::Filter(const pb::common::VectorScalardata& scalar_data, bool& is_reserved) {
  return RawCoprocessor::Filter(scalar_data, is_reserved);
}

void CoprocessorV2::Close() {}

butil::Status CoprocessorV2::DoExecute(const std::string& key, const std::string& value, bool* has_result_kv,
                                       pb::common::KeyValue* result_kv) {
  butil::Status status;

  std::unique_ptr<std::vector<expr::Operand>> result_operand_ptr;

  status = DoRelExprCoreWrapper(key, value, result_operand_ptr);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << status.error_cstr();
    return status;
  }

  if (!result_operand_ptr) {
    *has_result_kv = false;
    return butil::Status();
  }

  std::vector<std::any> result_record;
  status = RelExprHelper::TransFromOperandWrapper(result_operand_ptr, result_serial_schemas_, result_column_indexes_,
                                                  result_record);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << status.error_cstr();
    return status;
  }

  status = GetKvFromExpr(result_record, has_result_kv, result_kv);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << status.error_cstr();
    return status;
  }

  return status;
}

butil::Status CoprocessorV2::DoFilter(const std::string& key, const std::string& value, bool* is_reserved) {
  butil::Status status;

  std::unique_ptr<std::vector<expr::Operand>> result_operand_ptr;

  status = DoRelExprCoreWrapper(key, value, result_operand_ptr);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << status.error_cstr();
    return status;
  }

  if (!result_operand_ptr) {
    *is_reserved = false;
    return butil::Status();
  }

  *is_reserved = true;

  return status;
}

butil::Status CoprocessorV2::DoRelExprCore(const std::vector<std::any>& original_record,
                                           std::unique_ptr<std::vector<expr::Operand>>& result_operand_ptr) {
  butil::Status status;

  std::unique_ptr<std::vector<expr::Operand>> operand_ptr = std::make_unique<std::vector<expr::Operand>>();
  status = RelExprHelper::TransToOperandWrapper(original_serial_schemas_, selection_column_indexes_, original_record,
                                                operand_ptr);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << status.error_cstr();
    return status;
  }

  try {
    std::vector<expr::Operand>* raw_operand_ptr = operand_ptr.release();

    const expr::Tuple* result_tuple = rel_runner_->Put(raw_operand_ptr);
    result_operand_ptr.reset(const_cast<expr::Tuple*>(result_tuple));
  } catch (const std::exception& my_exception) {
    std::string error_message = fmt::format("expr::RelRunner Put failed. exception : {}", my_exception.what());
    DINGO_LOG(ERROR) << error_message;
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, error_message);
  }

  return status;
}

butil::Status CoprocessorV2::DoRelExprCoreWrapper(const std::string& key, const std::string& value,
                                                  std::unique_ptr<std::vector<expr::Operand>>& result_operand_ptr) {
  butil::Status status;

  std::vector<std::any> original_record;

  int ret = 0;
  try {
    // decode some column. not decode all
    ret = original_record_decoder_->Decode(key, value, selection_column_indexes_, original_record);
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

  return DoRelExprCore(original_record, result_operand_ptr);
}

butil::Status CoprocessorV2::GetKvFromExprEndOfFinish(bool /*key_only*/, size_t /*max_fetch_cnt*/,
                                                      int64_t /*max_bytes_rpc*/,
                                                      std::vector<pb::common::KeyValue>* kvs) {
  butil::Status status;

  while (true) {
    std::unique_ptr<std::vector<expr::Operand>> result_operand_ptr;
    try {
      const expr::Tuple* result_tuple = rel_runner_->Get();
      result_operand_ptr.reset(const_cast<expr::Tuple*>(result_tuple));
    } catch (const std::exception& my_exception) {
      std::string error_message = fmt::format("expr::RelRunner Get failed. exception : {}", my_exception.what());
      DINGO_LOG(ERROR) << error_message;
      return butil::Status(pb::error::EILLEGAL_PARAMTETERS, error_message);
    }

    if (!result_operand_ptr) {
      break;
    }

    std::vector<std::any> result_record;
    status = RelExprHelper::TransFromOperandWrapper(result_operand_ptr, result_serial_schemas_, result_column_indexes_,
                                                    result_record);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << status.error_cstr();
      return status;
    }

    bool has_result_kv = false;
    pb::common::KeyValue result_kv;

    status = GetKvFromExpr(result_record, &has_result_kv, &result_kv);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << status.error_cstr();
      return status;
    }

    if (has_result_kv) {
      kvs->emplace_back(std::move(result_kv));
    }
  }

  return status;
}

butil::Status CoprocessorV2::GetKvFromExpr(const std::vector<std::any>& record, bool* has_result_kv,
                                           pb::common::KeyValue* result_kv) {
  butil::Status status;

  pb::common::KeyValue result_key_value;
  int ret = 0;
  try {
    ret = result_record_encoder_->Encode(record, result_key_value);
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

void CoprocessorV2::GetOriginalColumnIndexes() {
  original_column_indexes_.resize(original_serial_schemas_->size(), -1);
  int i = 0;
  for (const auto& schema : *original_serial_schemas_) {
    int index = schema->GetIndex();
    DINGO_LOG(DEBUG) << index << "," << i;
    original_column_indexes_[index] = i;
    i++;
  }
}

void CoprocessorV2::GetSelectionColumnIndexes() {
  if (!coprocessor_.selection_columns().empty()) {
    for (const auto& index : coprocessor_.selection_columns()) {
      int i = index;
      DINGO_LOG(DEBUG) << "index:" << i;
      selection_column_indexes_.push_back(original_column_indexes_[i]);
    }
  } else {
    DINGO_LOG(DEBUG) << "selection_columns empty()";
  }
}

void CoprocessorV2::GetResultColumnIndexes() {
  result_column_indexes_.resize(result_serial_schemas_->size(), -1);
  int i = 0;
  for (const auto& schema : *result_serial_schemas_) {
    int index = schema->GetIndex();
    DINGO_LOG(DEBUG) << index << "," << i;
    result_column_indexes_[index] = i;
    i++;
  }
}

void CoprocessorV2::ShowOriginalColumnIndexes() {
  int i = 0;
  std::string s;
  s.reserve(1024);
  for (int value : original_column_indexes_) {
    s += "original member index : " + std::to_string(i) + " -> " + "array index : " + std::to_string(value) + "\n";
    i++;
  }
  DINGO_LOG(DEBUG) << fmt::format("original_column_indexes :\n{}", s);
}

void CoprocessorV2::ShowSelectionColumnIndexes() {
  int i = 0;
  std::string s;
  s.reserve(1024);
  for (int value : selection_column_indexes_) {
    s += "dummy :" + std::to_string(i) + " -> " + "original array index :" + std::to_string(value) + "\n";
    i++;
  }
  DINGO_LOG(DEBUG) << fmt::format("selection_column_indexes :\n{}", s);
}

void CoprocessorV2::ShowResultColumnIndexes() {
  int i = 0;
  std::string s;
  s.reserve(1024);
  for (int value : result_column_indexes_) {
    s += "result member index : " + std::to_string(i) + " -> " + "array index : " + std::to_string(value) + "\n";
    i++;
  }
  DINGO_LOG(DEBUG) << fmt::format("result_column_indexes :\n{}", s);
}

}  // namespace dingodb
