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

#include "coprocessor/raw_coprocessor.h"

#include <cstddef>
#include <cstdint>

namespace dingodb {

RawCoprocessor::RawCoprocessor() = default;
RawCoprocessor::~RawCoprocessor() = default;

butil::Status RawCoprocessor::Open(const std::any& /*coprocessor*/) {
  butil::Status status(pb::error::ENOT_SUPPORT, "Not Support");

  return butil::Status();
}

butil::Status RawCoprocessor::Execute(IteratorPtr /*iter*/, bool /*key_only*/, size_t /*max_fetch_cnt*/,
                                      int64_t /*max_bytes_rpc*/, std::vector<pb::common::KeyValue>* /*kvs*/,
                                      bool& /*has_more*/) {
  butil::Status status(pb::error::ENOT_SUPPORT, "Not Support");

  return status;
}

butil::Status RawCoprocessor::Execute(TxnIteratorPtr /*iter*/, int64_t /*limit*/, bool /*key_only*/,
                                      bool /*is_reverse*/, pb::store::TxnResultInfo& /*txn_result_info*/,
                                      std::vector<pb::common::KeyValue>& /*kvs*/, bool& /*has_more*/,
                                      std::string& /*end_key*/) {
  butil::Status status(pb::error::ENOT_SUPPORT, "Not Support");

  return status;
}

butil::Status RawCoprocessor::Filter(const std::string& /*key*/, const std::string& /*value*/, bool& /*is_reserved*/) {
  butil::Status status(pb::error::ENOT_SUPPORT, "Not Support");

  return status;
}

butil::Status RawCoprocessor::Filter(const pb::common::VectorScalardata& /*scalar_data*/, bool& /*is_reserved*/) {
  butil::Status status(pb::error::ENOT_SUPPORT, "Not Support");

  return status;
}
void RawCoprocessor::Close() {}

}  // namespace dingodb
