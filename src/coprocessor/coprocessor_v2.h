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

#ifndef DINGODB_COPROCESSOR_COPROCESSOR_V2_H_  // NOLINT
#define DINGODB_COPROCESSOR_COPROCESSOR_V2_H_

#include <serial/schema/base_schema.h>

#include <any>
#include <functional>
#include <memory>
#include <string>
#include <vector>
#include <unordered_map>

#include "butil/status.h"
#include "coprocessor/raw_coprocessor.h"
#include "coprocessor/rel_expr_helper.h"  // IWYU pragma: keep
#include "engine/iterator.h"
#include "glog/logging.h"
#include "proto/common.pb.h"
#include "rel/rel_runner.h"  // IWYU pragma: keep
#include "serial/record_decoder.h"
#include "serial/record_encoder.h"

namespace dingodb {

DECLARE_bool(enable_coprocessor_v2_statistics_time_consumption);

class CoprocessorV2;
using CoprocessorV2Ptr = std::shared_ptr<CoprocessorV2>;

class CoprocessorV2 : public RawCoprocessor {
 public:
  CoprocessorV2(char prefix);
  ~CoprocessorV2() override;

  CoprocessorV2(const CoprocessorV2& rhs) = delete;
  CoprocessorV2& operator=(const CoprocessorV2& rhs) = delete;
  CoprocessorV2(CoprocessorV2&& rhs) = delete;
  CoprocessorV2& operator=(CoprocessorV2&& rhs) = delete;

  static CoprocessorV2Ptr New(char prefix) { return std::make_shared<CoprocessorV2>(prefix); }

  // coprocessor = CoprocessorPbWrapper
  butil::Status Open(const std::any& coprocessor) override;

  butil::Status Execute(IteratorPtr iter, bool key_only, size_t max_fetch_cnt, int64_t max_bytes_rpc,
                        std::vector<pb::common::KeyValue>* kvs, bool& has_more) override;

  butil::Status Execute(TxnIteratorPtr iter, bool key_only, bool is_reverse, StopChecker& stop_checker,
                        pb::store::TxnResultInfo& txn_result_info, std::vector<pb::common::KeyValue>& kvs,
                        bool& has_more) override;

  butil::Status Filter(const std::string& key, const std::string& value, bool& is_reserved) override;  // NOLINT

  butil::Status Filter(const pb::common::VectorScalardata& scalar_data, bool& is_reserved) override;  // NOLINT

  void Close() override;

 protected:
  butil::Status DoExecute(const std::string& key, const std::string& value, bool* has_result_kv,
                          pb::common::KeyValue* result_kv);
  butil::Status DoFilter(const std::string& key, const std::string& value, bool* is_reserved);
  butil::Status DoRelExprCore(int codec_version, const std::vector<std::any>& original_record,
                              std::unique_ptr<std::vector<expr::Operand>>& result_operand_ptr);  // NOLINT
  butil::Status DoRelExprCoreWrapper(const std::string& key, const std::string& value,
                                     std::unique_ptr<std::vector<expr::Operand>>& result_operand_ptr);  // NOLINT
  butil::Status GetKvFromExprEndOfFinish(std::vector<pb::common::KeyValue>* kvs);
  butil::Status GetKvFromExpr(const std::vector<std::any>& record, bool* has_result_kv,
                              pb::common::KeyValue* result_kv);

  void GetOriginalColumnIndexes();
  void GetSelectionColumnIndexes();
  void GetResultColumnIndexes();

  void ShowOriginalColumnIndexes();
  void ShowSelectionColumnIndexes();
  void ShowResultColumnIndexes();

  int GetCodecVersion(const std::string& key) {
    if (key.empty()) {
      throw std::runtime_error("key should not be empty in func GetCodecVersion.");
    }

    return key.c_str()[key.length() - 1];
  }

  char prefix_;                                                                        // NOLINT
  pb::common::CoprocessorV2 coprocessor_;                                              // NOLINT
  std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>> original_serial_schemas_;  // NOLINT
  // array index =  original schema member index field ; value = original schema array index
  std::vector<int> original_column_indexes_;  // NOLINT
  // index = dummy ; value =  original schema index
  std::vector<int> selection_column_indexes_;                                        // NOLINT
  std::unordered_map<int, int> selection_column_indexes_serial_;                                 // NOLINT
  std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>> result_serial_schemas_;  // NOLINT
  std::shared_ptr<RecordEncoder> result_record_encoder_;                             // NOLINT
  std::shared_ptr<RecordDecoder> original_record_decoder_;                           // NOLINT
  // array index =  result schema member index field ; value = result schema array index
  std::vector<int> result_column_indexes_;  // NOLINT
  bool forAggCount_; // NOLINT

#if defined(TEST_COPROCESSOR_V2_MOCK)
  std::shared_ptr<rel::mock::RelRunner> rel_runner_;  // NOLINT
#else
  std::shared_ptr<rel::RelRunner> rel_runner_;  // NOLINT
#endif

  static bvar::Adder<uint64_t> bvar_coprocessor_v2_object_running_num;
  static bvar::Adder<uint64_t> bvar_coprocessor_v2_object_total_num;
  static bvar::LatencyRecorder coprocessor_v2_latency;
  static bvar::Adder<uint64_t> bvar_coprocessor_v2_execute_running_num;
  static bvar::Adder<uint64_t> bvar_coprocessor_v2_execute_total_num;
  static bvar::LatencyRecorder coprocessor_v2_execute_latency;
  static bvar::Adder<uint64_t> bvar_coprocessor_v2_execute_txn_running_num;
  static bvar::Adder<uint64_t> bvar_coprocessor_v2_execute_txn_total_num;
  static bvar::LatencyRecorder coprocessor_v2_execute_txn_latency;
  static bvar::Adder<uint64_t> bvar_coprocessor_v2_filter_running_num;
  static bvar::Adder<uint64_t> bvar_coprocessor_v2_filter_total_num;
  static bvar::LatencyRecorder coprocessor_v2_filter_latency;

  BvarLatencyGuard bvar_guard_for_coprocessor_v2_latency_;  // NOLINT

  // coprocessor_v2_statistics_time_consumption
  std::chrono::steady_clock::time_point coprocessor_v2_start_time_point;
  std::chrono::steady_clock::time_point coprocessor_v2_end_time_point;
  int64_t coprocessor_v2_spend_time_ms;
  int64_t iter_next_spend_time_ms;
  int64_t get_kv_spend_time_ms;
  int64_t trans_field_spend_time_ms;
  int64_t decode_spend_time_ms;
  int64_t rel_expr_spend_time_ms;
  int64_t misc_spend_time_ms;
  int64_t open_spend_time_ms;
};

}  // namespace dingodb

#endif  // DINGODB_COPROCESSOR_COPROCESSOR_V2_H_  // NOLINT
