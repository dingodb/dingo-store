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

#ifndef DINGODB_COPROCESSOR_AGGREGATION_H_  // NOLINT
#define DINGODB_COPROCESSOR_AGGREGATION_H_

#include <serial/schema/base_schema.h>

#include <any>
#include <functional>
#include <memory>
#include <vector>

#include "butil/status.h"
#include "proto/store.pb.h"

namespace dingodb {

class Aggregation {
 public:
  Aggregation();
  ~Aggregation();

  Aggregation(const Aggregation& rhs) = delete;
  Aggregation& operator=(const Aggregation& rhs) = delete;
  Aggregation(Aggregation&& rhs) = delete;
  Aggregation& operator=(Aggregation&& rhs) = delete;

  butil::Status Open(
      size_t start_aggregation_operators_index,
      const std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>>& result_serial_schemas,
      const ::google::protobuf::RepeatedPtrField<pb::store::AggregationOperator>& aggregation_operators);

  butil::Status Execute(const std::vector<std::function<bool(const std::any&, std::any*)>>& aggregation_functions,
                        const std::vector<std::any>& group_by_operator_record);

  const std::shared_ptr<std::vector<std::any>>& GetResult() const { return result_record_; }

  void Close();

 private:
  std::shared_ptr<std::vector<std::any>> result_record_;
};

}  // namespace dingodb

#endif  // DINGODB_COPROCESSOR_AGGREGATION_H_  // NOLINT
