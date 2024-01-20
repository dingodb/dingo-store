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

#ifndef DINGODB_RAW_COPROCESSOR_COPROCESSOR_H_  // NOLINT
#define DINGODB_RAW_COPROCESSOR_COPROCESSOR_H_

#include <any>
#include <memory>
#include <variant>
#include <vector>

#include "butil/status.h"
#include "engine/iterator.h"
#include "engine/txn_engine_helper.h"
#include "proto/store.pb.h"

namespace dingodb {

using CoprocessorPbWrapper = std::variant<pb::store::Coprocessor, pb::common::CoprocessorV2>;

class TxnIterator;

using TxnIteratorPtr = std::shared_ptr<TxnIterator>;

class RawCoprocessor {
 public:
  RawCoprocessor();
  virtual ~RawCoprocessor();

  RawCoprocessor(const RawCoprocessor& rhs) = delete;
  RawCoprocessor& operator=(const RawCoprocessor& rhs) = delete;
  RawCoprocessor(RawCoprocessor&& rhs) = delete;
  RawCoprocessor& operator=(RawCoprocessor&& rhs) = delete;

  // coprocessor = CoprocessorPbWrapper
  virtual butil::Status Open(const std::any& coprocessor);

  virtual butil::Status Execute(IteratorPtr iter, bool key_only, size_t max_fetch_cnt, int64_t max_bytes_rpc,
                                std::vector<pb::common::KeyValue>* kvs, bool& has_more);

  virtual butil::Status Execute(TxnIteratorPtr iter, int64_t limit, bool key_only, bool is_reverse,
                                pb::store::TxnResultInfo& txn_result_info,
                                std::vector<pb::common::KeyValue>& kvs,  // NOLINT
                                bool& has_more, std::string& end_key);   // NOLINT

  virtual butil::Status Filter(const std::string& key, const std::string& value, bool& is_reserved);  // NOLINT

  virtual butil::Status Filter(const pb::common::VectorScalardata& scalar_data, bool& is_reserved);  // NOLINT

  virtual void Close();
};

}  // namespace dingodb

#endif  // DINGODB_RAW_COPROCESSOR_COPROCESSOR_H_  // NOLINT
