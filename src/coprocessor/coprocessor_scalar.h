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

#ifndef DINGODB_COPROCESSOR_COPROCESSOR_SCALAR_H_  // NOLINT
#define DINGODB_COPROCESSOR_COPROCESSOR_SCALAR_H_

#include <serial/schema/base_schema.h>

#include <any>
#include <vector>

#include "butil/status.h"
#include "coprocessor/coprocessor_v2.h"
#include "engine/iterator.h"
#include "proto/common.pb.h"

namespace dingodb {

class CoprocessorScalar : public CoprocessorV2 {
 public:
  CoprocessorScalar();
  ~CoprocessorScalar() override;

  CoprocessorScalar(const CoprocessorScalar& rhs) = delete;
  CoprocessorScalar& operator=(const CoprocessorScalar& rhs) = delete;
  CoprocessorScalar(CoprocessorScalar&& rhs) = delete;
  CoprocessorScalar& operator=(CoprocessorScalar&& rhs) = delete;

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
  butil::Status TransToAnyRecord(const pb::common::VectorScalardata& scalar_data,
                                 std::vector<std::any>& original_record);  // NOLINT
  static bvar::Adder<uint64_t> bvar_coprocessor_v2_filter_scalar_running_num;
  static bvar::Adder<uint64_t> bvar_coprocessor_v2_filter_scalar_total_num;
  static bvar::LatencyRecorder coprocessor_v2_filter_scalar_latency;
};

}  // namespace dingodb

#endif  // DINGODB_COPROCESSOR_COPROCESSOR_SCALAR_H_  // NOLINT
