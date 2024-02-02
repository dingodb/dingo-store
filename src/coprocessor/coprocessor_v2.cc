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
#include "fmt/core.h"
#include "proto/error.pb.h"
#include "proto/store.pb.h"
#include "scan/scan_filter.h"
#include "serial/record_decoder.h"
#include "serial/record_encoder.h"

namespace dingodb {

DECLARE_int64(max_scan_memory_size);
DECLARE_int64(max_scan_line_limit);

bvar::Adder<uint64_t> CoprocessorV2::bvar_coprocessor_v2_object_running_num("dingo_coprocessor_v2_object_running_num");
bvar::Adder<uint64_t> CoprocessorV2::bvar_coprocessor_v2_object_total_num("dingo_coprocessor_v2_object_total_num");
bvar::LatencyRecorder CoprocessorV2::coprocessor_v2_latency("dingo_coprocessor_v2_latency");
bvar::Adder<uint64_t> CoprocessorV2::bvar_coprocessor_v2_execute_running_num(
    "dingo_coprocessor_v2_execute_running_num");
bvar::Adder<uint64_t> CoprocessorV2::bvar_coprocessor_v2_execute_total_num("dingo_coprocessor_v2_execute_total_num");
bvar::LatencyRecorder CoprocessorV2::coprocessor_v2_execute_latency("dingo_coprocessor_v2_execute_latency");
bvar::Adder<uint64_t> CoprocessorV2::bvar_coprocessor_v2_execute_txn_running_num(
    "dingo_coprocessor_v2_execute_txn_running_num");
bvar::Adder<uint64_t> CoprocessorV2::bvar_coprocessor_v2_execute_txn_total_num(
    "dingo_coprocessor_v2_execute_txn_total_num");
bvar::LatencyRecorder CoprocessorV2::coprocessor_v2_execute_txn_latency("dingo_coprocessor_v2_execute_txn_latency");
bvar::Adder<uint64_t> CoprocessorV2::bvar_coprocessor_v2_filter_running_num("dingo_coprocessor_v2_filter_running_num");
bvar::Adder<uint64_t> CoprocessorV2::bvar_coprocessor_v2_filter_total_num("dingo_coprocessor_v2_filter_total_num");
bvar::LatencyRecorder CoprocessorV2::coprocessor_v2_filter_latency("dingo_coprocessor_v2_filter_latency");

CoprocessorV2::CoprocessorV2()
    : bvar_guard_for_coprocessor_v2_latency_(&coprocessor_v2_latency)
#if defined(ENABLE_COPROCESSOR_V2_STATISTICS_TIME_CONSUMPTION)
      ,
      coprocessor_v2_start_time_point(std::chrono::steady_clock::now()),
      coprocessor_v2_spend_time_ms(0),
      iter_next_spend_time_ms(0),
      get_kv_spend_time_ms(0),
      trans_field_spend_time_ms(0),
      decode_spend_time_ms(0),
      rel_expr_spend_time_ms(0),
      misc_spend_time_ms(0),
      open_spend_time_ms(0)
#endif
{
  bvar_coprocessor_v2_object_running_num << 1;
  bvar_coprocessor_v2_object_total_num << 1;
};
CoprocessorV2::~CoprocessorV2() {
  Close();
  bvar_coprocessor_v2_object_running_num << -1;
#if defined(ENABLE_COPROCESSOR_V2_STATISTICS_TIME_CONSUMPTION)
  // coprocessor_v2_end_time_point = std::chrono::steady_clock::now();
  coprocessor_v2_spend_time_ms = std::chrono::duration_cast<std::chrono::microseconds>(coprocessor_v2_end_time_point -
                                                                                       coprocessor_v2_start_time_point)
                                     .count();
  misc_spend_time_ms = coprocessor_v2_spend_time_ms - iter_next_spend_time_ms - get_kv_spend_time_ms -
                       trans_field_spend_time_ms - decode_spend_time_ms - rel_expr_spend_time_ms - open_spend_time_ms;
  DINGO_LOG(INFO) << fmt::format(
      "CoprocessorV2 time_consumption total:{}ms, iter next:{}ms, get kv:{}ms, trans field:{}ms, decode and "
      "encode:{}ms, rel expr:{}ms, misc:{}ms, open:{}ms",
      coprocessor_v2_spend_time_ms, iter_next_spend_time_ms, get_kv_spend_time_ms, trans_field_spend_time_ms,
      decode_spend_time_ms, rel_expr_spend_time_ms, misc_spend_time_ms, open_spend_time_ms);

  DINGO_LOG(INFO) << fmt::format(
      "CoprocessorV2 time_consumption percent:  iter next:{}%, get kv:{}% trans field:{}%, decode and "
      "encode:{}%, rel expr:{}%, misc:{}%, open:{}%",
      static_cast<double>(iter_next_spend_time_ms) / coprocessor_v2_spend_time_ms * 100,
      static_cast<double>(get_kv_spend_time_ms) / coprocessor_v2_spend_time_ms * 100,
      static_cast<double>(trans_field_spend_time_ms) / coprocessor_v2_spend_time_ms * 100,
      static_cast<double>(decode_spend_time_ms) / coprocessor_v2_spend_time_ms * 100,
      static_cast<double>(rel_expr_spend_time_ms) / coprocessor_v2_spend_time_ms * 100,
      static_cast<double>(misc_spend_time_ms) / coprocessor_v2_spend_time_ms * 100,
      static_cast<double>(open_spend_time_ms) / coprocessor_v2_spend_time_ms * 100);
#endif
}

butil::Status CoprocessorV2::Open(const std::any& coprocessor) {
#if defined(ENABLE_COPROCESSOR_V2_STATISTICS_TIME_CONSUMPTION)
  auto lambda_time_now_function = []() { return std::chrono::steady_clock::now(); };
  auto lambda_time_diff_microseconds_function = [](auto start, auto end) {
    return std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
  };

  auto open_start = lambda_time_now_function();
  ON_SCOPE_EXIT([&]() {
    auto open_end = lambda_time_now_function();
    open_spend_time_ms += lambda_time_diff_microseconds_function(open_start, open_end);
  });
#endif
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

#if defined(TEST_COPROCESSOR_V2_MOCK)
  rel_runner_ = std::make_shared<rel::mock::RelRunner>();
#else
  rel_runner_ = std::make_shared<rel::RelRunner>();
#endif

  try {
    rel_runner_->Decode(reinterpret_cast<const expr::Byte*>(coprocessor_.rel_expr().c_str()),
                        coprocessor_.rel_expr().length());
  } catch (const std::exception& my_exception) {
    std::string error_message = fmt::format("rel::RelRunner Decode failed. exception : {}", my_exception.what());
    DINGO_LOG(ERROR) << error_message;
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, error_message);
  }

  return status;
}

butil::Status CoprocessorV2::Execute(IteratorPtr iter, bool key_only, size_t max_fetch_cnt, int64_t max_bytes_rpc,
                                     std::vector<pb::common::KeyValue>* kvs, bool& has_more) {
#if defined(ENABLE_COPROCESSOR_V2_STATISTICS_TIME_CONSUMPTION)
  auto lambda_time_now_function = []() { return std::chrono::steady_clock::now(); };
  auto lambda_time_diff_microseconds_function = [](auto start, auto end) {
    return std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
  };
  ON_SCOPE_EXIT([&]() { coprocessor_v2_end_time_point = std::chrono::steady_clock::now(); });
#endif
  BvarLatencyGuard bvar_guard(&coprocessor_v2_execute_latency);
  CoprocessorV2::bvar_coprocessor_v2_execute_running_num << 1;
  CoprocessorV2::bvar_coprocessor_v2_execute_total_num << 1;
  ON_SCOPE_EXIT([&]() { CoprocessorV2::bvar_coprocessor_v2_execute_running_num << -1; });
  DINGO_LOG(DEBUG) << fmt::format("CoprocessorV2::Execute IteratorPtr Enter");
  ScanFilter scan_filter = ScanFilter(false, max_fetch_cnt, max_bytes_rpc);
  butil::Status status;
  has_more = false;
  while (iter->Valid()) {
    pb::common::KeyValue kv;
#if defined(ENABLE_COPROCESSOR_V2_STATISTICS_TIME_CONSUMPTION)
    {
      auto kv_start = lambda_time_now_function();
      ON_SCOPE_EXIT([&]() {
        auto kv_end = lambda_time_now_function();
        get_kv_spend_time_ms += lambda_time_diff_microseconds_function(kv_start, kv_end);
      });
#endif
      *kv.mutable_key() = iter->Key();
      *kv.mutable_value() = iter->Value();
#if defined(ENABLE_COPROCESSOR_V2_STATISTICS_TIME_CONSUMPTION)
    }
#endif
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
      has_more = true;
      DINGO_LOG(WARNING) << fmt::format(
          "CoprocessorV2 UptoLimit. key_only : {} max_fetch_cnt : {} max_bytes_rpc : {} cur_fetch_cnt : {} "
          "cur_bytes_rpc : {}",
          key_only, max_fetch_cnt, max_bytes_rpc, scan_filter.GetCurFetchCnt(), scan_filter.GetCurBytesRpc());
#if defined(ENABLE_COPROCESSOR_V2_STATISTICS_TIME_CONSUMPTION)
      {
        auto next_start = lambda_time_now_function();
        ON_SCOPE_EXIT([&]() {
          auto next_end = lambda_time_now_function();
          iter_next_spend_time_ms += lambda_time_diff_microseconds_function(next_start, next_end);
        });
#endif
        iter->Next();
#if defined(ENABLE_COPROCESSOR_V2_STATISTICS_TIME_CONSUMPTION)
      }
#endif
      break;
    }
#if defined(ENABLE_COPROCESSOR_V2_STATISTICS_TIME_CONSUMPTION)
    {
      auto next_start = lambda_time_now_function();
      ON_SCOPE_EXIT([&]() {
        auto next_end = lambda_time_now_function();
        iter_next_spend_time_ms += lambda_time_diff_microseconds_function(next_start, next_end);
      });
#endif
      iter->Next();
#if defined(ENABLE_COPROCESSOR_V2_STATISTICS_TIME_CONSUMPTION)
    }
#endif
  }

  status = GetKvFromExprEndOfFinish(key_only, max_fetch_cnt, max_bytes_rpc, kvs);

  DINGO_LOG(DEBUG) << fmt::format("CoprocessorV2::Execute IteratorPtr Leave");

  return status;
}

butil::Status CoprocessorV2::Execute(TxnIteratorPtr iter, int64_t limit, bool key_only, bool /*is_reverse*/,
                                     pb::store::TxnResultInfo& txn_result_info, std::vector<pb::common::KeyValue>& kvs,
                                     bool& has_more, std::string& end_key) {
#if defined(ENABLE_COPROCESSOR_V2_STATISTICS_TIME_CONSUMPTION)
  auto lambda_time_now_function = []() { return std::chrono::steady_clock::now(); };
  auto lambda_time_diff_microseconds_function = [](auto start, auto end) {
    return std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
  };
  ON_SCOPE_EXIT([&]() { coprocessor_v2_end_time_point = std::chrono::steady_clock::now(); });
#endif
  BvarLatencyGuard bvar_guard(&coprocessor_v2_execute_txn_latency);
  CoprocessorV2::bvar_coprocessor_v2_execute_txn_running_num << 1;
  CoprocessorV2::bvar_coprocessor_v2_execute_txn_total_num << 1;
  ON_SCOPE_EXIT([&]() { CoprocessorV2::bvar_coprocessor_v2_execute_txn_running_num << -1; });
  DINGO_LOG(DEBUG) << fmt::format("CoprocessorV2::Execute  TxnIteratorPtr Enter");

  butil::Status status;

  ScanFilter scan_filter =
      ScanFilter(false, std::min(limit, FLAGS_max_scan_line_limit), std::numeric_limits<int64_t>::max());

  while (iter->Valid(txn_result_info)) {
    pb::common::KeyValue kv;
#if defined(ENABLE_COPROCESSOR_V2_STATISTICS_TIME_CONSUMPTION)
    {
      auto kv_start = lambda_time_now_function();
      ON_SCOPE_EXIT([&]() {
        auto kv_end = lambda_time_now_function();
        get_kv_spend_time_ms += lambda_time_diff_microseconds_function(kv_start, kv_end);
      });
#endif
      *kv.mutable_key() = iter->Key();
      *kv.mutable_value() = iter->Value();
#if defined(ENABLE_COPROCESSOR_V2_STATISTICS_TIME_CONSUMPTION)
    }
#endif
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
          "CoprocessorV2 UptoLimit. key_only : {} max_fetch_cnt : {} max_bytes_rpc : {} cur_fetch_cnt : {} "
          "cur_bytes_rpc : {}",
          key_only, std::min(limit, FLAGS_max_scan_line_limit), std::numeric_limits<int64_t>::max(),
          scan_filter.GetCurFetchCnt(), scan_filter.GetCurBytesRpc());
#if defined(ENABLE_COPROCESSOR_V2_STATISTICS_TIME_CONSUMPTION)
      {
        auto next_start = lambda_time_now_function();
        ON_SCOPE_EXIT([&]() {
          auto next_end = lambda_time_now_function();
          iter_next_spend_time_ms += lambda_time_diff_microseconds_function(next_start, next_end);
        });
#endif
        iter->Next();
#if defined(ENABLE_COPROCESSOR_V2_STATISTICS_TIME_CONSUMPTION)
      }
#endif
      break;
    }
#if defined(ENABLE_COPROCESSOR_V2_STATISTICS_TIME_CONSUMPTION)
    {
      auto next_start = lambda_time_now_function();
      ON_SCOPE_EXIT([&]() {
        auto next_end = lambda_time_now_function();
        iter_next_spend_time_ms += lambda_time_diff_microseconds_function(next_start, next_end);
      });
#endif
      iter->Next();
#if defined(ENABLE_COPROCESSOR_V2_STATISTICS_TIME_CONSUMPTION)
    }
#endif
  }

  status = GetKvFromExprEndOfFinish(key_only, limit, FLAGS_max_scan_memory_size, &kvs);

  DINGO_LOG(DEBUG) << fmt::format("CoprocessorV2::Execute TxnIteratorPtr Leave");

  return status;
}

butil::Status CoprocessorV2::Filter(const std::string& key, const std::string& value, bool& is_reserved) {
#if defined(ENABLE_COPROCESSOR_V2_STATISTICS_TIME_CONSUMPTION)
  ON_SCOPE_EXIT([&]() { coprocessor_v2_end_time_point = std::chrono::steady_clock::now(); });
#endif
  BvarLatencyGuard bvar_guard(&coprocessor_v2_filter_latency);
  CoprocessorV2::bvar_coprocessor_v2_filter_running_num << 1;
  CoprocessorV2::bvar_coprocessor_v2_filter_total_num << 1;
  ON_SCOPE_EXIT([&]() { CoprocessorV2::bvar_coprocessor_v2_filter_running_num << -1; });
  return DoFilter(key, value, &is_reserved);
}

butil::Status CoprocessorV2::Filter(const pb::common::VectorScalardata& scalar_data, bool& is_reserved) {
  return RawCoprocessor::Filter(scalar_data, is_reserved);
}

void CoprocessorV2::Close() {
  coprocessor_.Clear();
  original_serial_schemas_.reset();
  original_column_indexes_.clear();
  selection_column_indexes_.clear();
  result_serial_schemas_.reset();
  result_record_encoder_.reset();
  original_record_decoder_.reset();
  result_column_indexes_.clear();
  rel_runner_.reset();
}

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
#if defined(ENABLE_COPROCESSOR_V2_STATISTICS_TIME_CONSUMPTION)
  auto lambda_time_now_function = []() { return std::chrono::steady_clock::now(); };
  auto lambda_time_diff_microseconds_function = [](auto start, auto end) {
    return std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
  };
  {
    auto trans_start = lambda_time_now_function();
    ON_SCOPE_EXIT([&]() {
      auto trans_end = lambda_time_now_function();
      trans_field_spend_time_ms += lambda_time_diff_microseconds_function(trans_start, trans_end);
    });
#endif
    status = RelExprHelper::TransFromOperandWrapper(result_operand_ptr, result_serial_schemas_, result_column_indexes_,
                                                    result_record);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << status.error_cstr();
      return status;
    }
#if defined(ENABLE_COPROCESSOR_V2_STATISTICS_TIME_CONSUMPTION)
  }
#endif

#if defined(ENABLE_COPROCESSOR_V2_STATISTICS_TIME_CONSUMPTION)
  auto encode_start = lambda_time_now_function();
  ON_SCOPE_EXIT([&]() {
    auto encode_end = lambda_time_now_function();
    decode_spend_time_ms += lambda_time_diff_microseconds_function(encode_start, encode_end);
  });
#endif

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

#if defined(TEST_COPROCESSOR_V2_MOCK)
  Utils::DebugPrintAnyArray(original_record, "From Decode");
#endif
  std::unique_ptr<std::vector<expr::Operand>> operand_ptr = std::make_unique<std::vector<expr::Operand>>();
#if defined(ENABLE_COPROCESSOR_V2_STATISTICS_TIME_CONSUMPTION)
  auto lambda_time_now_function = []() { return std::chrono::steady_clock::now(); };
  auto lambda_time_diff_microseconds_function = [](auto start, auto end) {
    return std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
  };
  {
    auto trans_start = lambda_time_now_function();
    ON_SCOPE_EXIT([&]() {
      auto trans_end = lambda_time_now_function();
      trans_field_spend_time_ms += lambda_time_diff_microseconds_function(trans_start, trans_end);
    });
#endif

    status = RelExprHelper::TransToOperandWrapper(original_serial_schemas_, selection_column_indexes_, original_record,
                                                  operand_ptr);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << status.error_cstr();
      return status;
    }

#if defined(ENABLE_COPROCESSOR_V2_STATISTICS_TIME_CONSUMPTION)
  }
#endif

#if defined(ENABLE_COPROCESSOR_V2_STATISTICS_TIME_CONSUMPTION)
  auto expr_start = lambda_time_now_function();
  ON_SCOPE_EXIT([&]() {
    auto expr_end = lambda_time_now_function();
    rel_expr_spend_time_ms += lambda_time_diff_microseconds_function(expr_start, expr_end);
  });
#endif

  try {
    std::vector<expr::Operand>* raw_operand_ptr = operand_ptr.release();

    const expr::Tuple* result_tuple = rel_runner_->Put(raw_operand_ptr);
    result_operand_ptr.reset(const_cast<expr::Tuple*>(result_tuple));
  } catch (const std::exception& my_exception) {
    std::string error_message = fmt::format("rel::RelRunner Put failed. exception : {}", my_exception.what());
    DINGO_LOG(ERROR) << error_message;
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, error_message);
  }

  return status;
}

butil::Status CoprocessorV2::DoRelExprCoreWrapper(const std::string& key, const std::string& value,
                                                  std::unique_ptr<std::vector<expr::Operand>>& result_operand_ptr) {
  butil::Status status;

  std::vector<std::any> original_record;
#if defined(ENABLE_COPROCESSOR_V2_STATISTICS_TIME_CONSUMPTION)
  {
    auto lambda_time_now_function = []() { return std::chrono::steady_clock::now(); };
    auto lambda_time_diff_microseconds_function = [](auto start, auto end) {
      return std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
    };
    auto decode_start = lambda_time_now_function();
    ON_SCOPE_EXIT([&]() {
      auto decode_end = lambda_time_now_function();
      decode_spend_time_ms += lambda_time_diff_microseconds_function(decode_start, decode_end);
    });
#endif
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
#if defined(ENABLE_COPROCESSOR_V2_STATISTICS_TIME_CONSUMPTION)
  }
#endif

  return DoRelExprCore(original_record, result_operand_ptr);
}

butil::Status CoprocessorV2::GetKvFromExprEndOfFinish(bool /*key_only*/, size_t /*max_fetch_cnt*/,
                                                      int64_t /*max_bytes_rpc*/,
                                                      std::vector<pb::common::KeyValue>* kvs) {
  butil::Status status;

#if defined(ENABLE_COPROCESSOR_V2_STATISTICS_TIME_CONSUMPTION)
  auto lambda_time_now_function = []() { return std::chrono::steady_clock::now(); };
  auto lambda_time_diff_microseconds_function = [](auto start, auto end) {
    return std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
  };
#endif

  while (true) {
    std::unique_ptr<std::vector<expr::Operand>> result_operand_ptr;
#if defined(ENABLE_COPROCESSOR_V2_STATISTICS_TIME_CONSUMPTION)
    {
      auto expr_start = lambda_time_now_function();
      ON_SCOPE_EXIT([&]() {
        auto expr_end = lambda_time_now_function();
        rel_expr_spend_time_ms += lambda_time_diff_microseconds_function(expr_start, expr_end);
      });
#endif
      try {
        const expr::Tuple* result_tuple = rel_runner_->Get();
        result_operand_ptr.reset(const_cast<expr::Tuple*>(result_tuple));
      } catch (const std::exception& my_exception) {
        std::string error_message = fmt::format("rel::RelRunner Get failed. exception : {}", my_exception.what());
        DINGO_LOG(ERROR) << error_message;
        return butil::Status(pb::error::EILLEGAL_PARAMTETERS, error_message);
      }
#if defined(ENABLE_COPROCESSOR_V2_STATISTICS_TIME_CONSUMPTION)
    }
#endif

    if (!result_operand_ptr) {
      break;
    }

    std::vector<std::any> result_record;
#if defined(ENABLE_COPROCESSOR_V2_STATISTICS_TIME_CONSUMPTION)
    {
      auto trans_start = lambda_time_now_function();
      ON_SCOPE_EXIT([&]() {
        auto trans_end = lambda_time_now_function();
        trans_field_spend_time_ms += lambda_time_diff_microseconds_function(trans_start, trans_end);
      });
#endif
      status = RelExprHelper::TransFromOperandWrapper(result_operand_ptr, result_serial_schemas_,
                                                      result_column_indexes_, result_record);
      if (!status.ok()) {
        DINGO_LOG(ERROR) << status.error_cstr();
        return status;
      }
#if defined(ENABLE_COPROCESSOR_V2_STATISTICS_TIME_CONSUMPTION)
    }
#endif

    bool has_result_kv = false;
    pb::common::KeyValue result_kv;
#if defined(ENABLE_COPROCESSOR_V2_STATISTICS_TIME_CONSUMPTION)
    {
      auto encode_start = lambda_time_now_function();
      ON_SCOPE_EXIT([&]() {
        auto encode_end = lambda_time_now_function();
        decode_spend_time_ms += lambda_time_diff_microseconds_function(encode_start, encode_end);
      });
#endif
      status = GetKvFromExpr(result_record, &has_result_kv, &result_kv);
      if (!status.ok()) {
        DINGO_LOG(ERROR) << status.error_cstr();
        return status;
      }
#if defined(ENABLE_COPROCESSOR_V2_STATISTICS_TIME_CONSUMPTION)
    }
#endif

    if (has_result_kv) {
      kvs->emplace_back(std::move(result_kv));
    }
  }

  return status;
}

butil::Status CoprocessorV2::GetKvFromExpr(const std::vector<std::any>& record, bool* has_result_kv,
                                           pb::common::KeyValue* result_kv) {
  butil::Status status;

#if defined(TEST_COPROCESSOR_V2_MOCK)
  Utils::DebugPrintAnyArray(record, "From Expr");
#endif

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
