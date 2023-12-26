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
#include <memory>
#include <string>
#include <vector>

#include "butil/status.h"
#include "coprocessor/raw_coprocessor.h"
#include "coprocessor/rel_expr_helper.h"
#include "engine/iterator.h"
#include "proto/common.pb.h"
#include "serial/record_decoder.h"
#include "serial/record_encoder.h"

namespace dingodb {

class CoprocessorV2 : public RawCoprocessor {
 public:
  CoprocessorV2();
  ~CoprocessorV2() override;

  CoprocessorV2(const CoprocessorV2& rhs) = delete;
  CoprocessorV2& operator=(const CoprocessorV2& rhs) = delete;
  CoprocessorV2(CoprocessorV2&& rhs) = delete;
  CoprocessorV2& operator=(CoprocessorV2&& rhs) = delete;

  // coprocessor = CoprocessorPbWrapper
  butil::Status Open(const std::any& coprocessor) override;

  butil::Status Execute(IteratorPtr iter, bool key_only, size_t max_fetch_cnt, int64_t max_bytes_rpc,
                        std::vector<pb::common::KeyValue>* kvs) override;

  butil::Status Execute(TxnIteratorPtr iter, int64_t limit, bool key_only, bool is_reverse,
                        pb::store::TxnResultInfo& txn_result_info, std::vector<pb::common::KeyValue>& kvs,  // NOLINT
                        bool& has_more, std::string& end_key) override;                                     // NOLINT

  butil::Status Filter(const std::string& key, const std::string& value, bool& is_reserved) override;  // NOLINT

  butil::Status Filter(const pb::common::VectorScalardata& scalar_data, bool& is_reserved) override;  // NOLINT

  void Close() override;

 protected:
  butil::Status DoExecute(const std::string& key, const std::string& value, bool* has_result_kv,
                          pb::common::KeyValue* result_kv);
  butil::Status DoFilter(const std::string& key, const std::string& value, bool* is_reserved);
  butil::Status DoRelExprCore(const std::vector<std::any>& original_record,
                              std::unique_ptr<std::vector<expr::Operand>>& result_operand_ptr);  // NOLINT
  butil::Status DoRelExprCoreWrapper(const std::string& key, const std::string& value,
                                     std::unique_ptr<std::vector<expr::Operand>>& result_operand_ptr);  // NOLINT
  butil::Status GetKvFromExprEndOfFinish(bool key_only, size_t max_fetch_cnt, int64_t max_bytes_rpc,
                                         std::vector<pb::common::KeyValue>* kvs);
  butil::Status GetKvFromExpr(const std::vector<std::any>& record, bool* has_result_kv,
                              pb::common::KeyValue* result_kv);

  void GetOriginalColumnIndexes();
  void GetSelectionColumnIndexes();
  void GetResultColumnIndexes();

  void ShowOriginalColumnIndexes();
  void ShowSelectionColumnIndexes();
  void ShowResultColumnIndexes();

  pb::common::CoprocessorV2 coprocessor_;                                              // NOLINT
  std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>> original_serial_schemas_;  // NOLINT
  // array index =  original schema member index field ; value = original schema array index
  std::vector<int> original_column_indexes_;  // NOLINT
  // index = dummy ; value =  original schema index
  std::vector<int> selection_column_indexes_;                                        // NOLINT
  std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>> result_serial_schemas_;  // NOLINT
  std::shared_ptr<RecordEncoder> result_record_encoder_;                             // NOLINT
  std::shared_ptr<RecordDecoder> original_record_decoder_;                           // NOLINT
  // array index =  result schema member index field ; value = result schema array index
  std::vector<int> result_column_indexes_;  // NOLINT

  std::shared_ptr<expr::RelRunner> rel_runner_;  // NOLINT
};

}  // namespace dingodb

#endif  // DINGODB_COPROCESSOR_COPROCESSOR_V2_H_  // NOLINT
