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

#ifndef DINGODB_COPROCESSOR_COPROCESSOR_H_  // NOLINT
#define DINGODB_COPROCESSOR_COPROCESSOR_H_

#include <serial/schema/base_schema.h>

#include <any>
#include <memory>
#include <string>
#include <vector>

#include "butil/status.h"
#include "coprocessor/aggregation_manager.h"
#include "coprocessor/raw_coprocessor.h"
#include "engine/iterator.h"
#include "proto/store.pb.h"

namespace dingodb {

class Coprocessor : public RawCoprocessor {
 public:
  Coprocessor();
  ~Coprocessor() override;

  Coprocessor(const Coprocessor& rhs) = delete;
  Coprocessor& operator=(const Coprocessor& rhs) = delete;
  Coprocessor(Coprocessor&& rhs) = delete;
  Coprocessor& operator=(Coprocessor&& rhs) = delete;

  // coprocessor = CoprocessorPbWrapper
  butil::Status Open(const std::any& coprocessor) override;

  butil::Status Execute(IteratorPtr iter, bool key_only, size_t max_fetch_cnt, int64_t max_bytes_rpc,
                        std::vector<pb::common::KeyValue>* kvs, bool& has_more) override;

  butil::Status Execute(TxnIteratorPtr iter, int64_t limit, bool key_only, bool is_reverse,
                        pb::store::TxnResultInfo& txn_result_info, std::vector<pb::common::KeyValue>& kvs,  // NOLINT
                        bool& has_more, std::string& end_key) override;                                     // NOLINT

  butil::Status Filter(const std::string& key, const std::string& value, bool& is_reserved) override;  // NOLINT

  butil::Status Filter(const pb::common::VectorScalardata& scalar_data, bool& is_reserved) override;  // NOLINT

  void Close() override;

 private:
  butil::Status DoExecute(const pb::common::KeyValue& kv, bool* has_result_kv, pb::common::KeyValue* result_kv);

  butil::Status DoExecuteForAggregation(const std::vector<std::any>& selection_record);

  butil::Status DoExecuteForSelection(const std::vector<std::any>& selection_record, bool* has_result_kv,
                                      pb::common::KeyValue* result_kv);
  butil::Status GetKeyValueFromAggregation(bool key_only, size_t max_fetch_cnt, int64_t max_bytes_rpc,
                                           std::vector<pb::common::KeyValue>* kvs);

  butil::Status CompareSerialSchema(const pb::store::Coprocessor& coprocessor);

  butil::Status InitGroupBySerialSchema(const pb::store::Coprocessor& coprocessor);

  void GetOriginalColumnIndexes();
  void GetSelectionColumnIndexes();

  pb::store::Coprocessor coprocessor_;
  std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>> original_serial_schemas_;
  std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>> selection_serial_schemas_;
  // such as  group by a, b ..
  std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>> group_by_key_serial_schemas_;
  // such as SUM(c), count(d) ...
  std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>> group_by_operator_serial_schemas_;
  // such as SUM(c), count(d)  group by a, b
  std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>> group_by_serial_schemas_;
  std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>> result_serial_schemas_;
  bool enable_expression_;
  bool end_of_group_by_;
  std::shared_ptr<AggregationManager> aggregation_manager_;
  std::shared_ptr<AggregationIterator> aggregation_iterator_;
  std::vector<int> original_column_indexes_;
  std::vector<int> selection_column_indexes_;

  std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>> original_serial_schemas_sorted_;
  std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>> selection_serial_schemas_sorted_;
  std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>> result_serial_schemas_sorted_;

  static bvar::Adder<uint64_t> bvar_coprocessor_v1_object_running_num;
  static bvar::Adder<uint64_t> bvar_coprocessor_v1_object_total_num;
  static bvar::LatencyRecorder coprocessor_v1_latency;
  static bvar::Adder<uint64_t> bvar_coprocessor_v1_execute_running_num;
  static bvar::Adder<uint64_t> bvar_coprocessor_v1_execute_total_num;
  static bvar::LatencyRecorder coprocessor_v1_execute_latency;

  BvarLatencyGuard bvar_guard_for_coprocessor_v1_latency_;
};

}  // namespace dingodb

#endif  // DINGODB_COPROCESSOR_COPROCESSOR_H_  // NOLINT
