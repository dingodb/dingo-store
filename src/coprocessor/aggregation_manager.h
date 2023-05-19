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

#ifndef DINGODB_COPROCESSOR_AGGREGATIONMANAGER_MANAGER_H_  // NOLINT
#define DINGODB_COPROCESSOR_AGGREGATIONMANAGER_MANAGER_H_

#include <serial/schema/base_schema.h>

#include <any>
#include <functional>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "butil/status.h"
#include "coprocessor/aggregation.h"
#include "proto/store.pb.h"

namespace dingodb {

class AggregationIterator {
 public:
  explicit AggregationIterator(const std::shared_ptr<std::map<std::string, std::shared_ptr<Aggregation>>>& aggregations)
      : aggregations_(aggregations) {
    iter_ = aggregations_->begin();
  }

  ~AggregationIterator() { aggregations_.reset(); }

  bool HasNext() { return (iter_ != aggregations_->end()); }
  void Next() { ++iter_; }
  const std::string& GetKey() const { return iter_->first; }
  const std::shared_ptr<std::vector<std::any>>& GetValue() const { return iter_->second->GetResult(); }

 private:
  std::shared_ptr<std::map<std::string, std::shared_ptr<Aggregation>>> aggregations_;
  std::map<std::string, std::shared_ptr<Aggregation>>::iterator iter_;
};

class AggregationManager {
 public:
  AggregationManager();
  ~AggregationManager();

  AggregationManager(const AggregationManager& rhs) = delete;
  AggregationManager& operator=(const AggregationManager& rhs) = delete;
  AggregationManager(AggregationManager&& rhs) = delete;
  AggregationManager& operator=(AggregationManager&& rhs) = delete;

  butil::Status Open(const std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>>& group_by_operator_serial_schemas,
                     const ::google::protobuf::RepeatedPtrField<pb::store::AggregationOperator>& aggregation_operators,
                     const std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>>& result_serial_schemas);

  butil::Status Execute(const std::string& group_by_key, const std::vector<std::any>& group_by_operator_record);

  std::shared_ptr<AggregationIterator> CreateIterator();

  void Close();

 private:
  butil::Status AddSumFunction(BaseSchema::Type serial_schema_type, BaseSchema::Type result_schema_type);
  butil::Status AddCountFunction(BaseSchema::Type serial_schema_type, BaseSchema::Type result_schema_type);
  butil::Status AddCountWithNullFunction(BaseSchema::Type serial_schema_type, BaseSchema::Type result_schema_type);
  butil::Status AddMaxFunction(BaseSchema::Type serial_schema_type, BaseSchema::Type result_schema_type);
  butil::Status AddMinFunction(BaseSchema::Type serial_schema_type, BaseSchema::Type result_schema_type);

  std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>> group_by_operator_serial_schemas_;
  ::google::protobuf::RepeatedPtrField<pb::store::AggregationOperator> aggregation_operators_;
  std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>> result_serial_schemas_;
  std::vector<std::function<bool(const std::any&, std::any*)>> aggregation_functions_;
  std::shared_ptr<std::map<std::string, std::shared_ptr<Aggregation>>> aggregations_;
};

}  // namespace dingodb

#endif  // DINGODB_COPROCESSOR_AGGREGATIONMANAGER_MANAGER_H_  // NOLINT
