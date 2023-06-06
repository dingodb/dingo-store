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

#ifndef DINGODB_COPROCESSOR_UTILS_H_  // NOLINT
#define DINGODB_COPROCESSOR_UTILS_H_

#include <serial/schema/base_schema.h>

#include <any>
#include <memory>
#include <string>
#include <vector>

#include "butil/status.h"
#include "proto/store.pb.h"

namespace dingodb {

class Utils {
 public:
  Utils() = delete;
  ~Utils() = delete;

  Utils(const Utils& rhs) = delete;
  Utils& operator=(const Utils& rhs) = delete;
  Utils(Utils&& rhs) = delete;
  Utils& operator=(Utils&& rhs) = delete;

  static butil::Status CheckPbSchema(const google::protobuf::RepeatedPtrField<pb::store::Schema>& pb_schemas);
  static butil::Status CheckSerialSchema(
      const std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>>& serial_schemas);
  static butil::Status CheckSelection(const ::google::protobuf::RepeatedField<int32_t>& selection_columns,
                                      size_t original_schema_size);

  static butil::Status CheckGroupByColumns(const ::google::protobuf::RepeatedField<int32_t>& group_by_columns,
                                           size_t selection_columns_size);

  static butil::Status CheckGroupByOperators(
      const ::google::protobuf::RepeatedPtrField<pb::store::AggregationOperator>& aggregation_operators,
      size_t selection_columns_size);

  static butil::Status TransToSerialSchema(const google::protobuf::RepeatedPtrField<pb::store::Schema>& pb_schemas,
                                           std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>>* serial_schemas);

  static std::shared_ptr<BaseSchema> CloneSerialSchema(const std::shared_ptr<BaseSchema>& serial_schema);

  static butil::Status CreateSerialSchema(
      const std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>>& old_serial_schemas,
      const ::google::protobuf::RepeatedField<int32_t>& new_columns,
      std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>>* new_serial_schemas);

  static butil::Status UpdateSerialSchemaIndex(
      std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>>* serial_schemas);

  static butil::Status UpdateSerialSchemaKey(const std::vector<bool>& keys,
                                             std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>>* serial_schemas);

  static butil::Status JoinSerialSchema(
      const std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>>& old_serial_schemas1,
      const std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>>& old_serial_schemas2,
      std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>>* new_serial_schemas);

  static butil::Status CompareSerialSchemaStrict(
      const std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>>& serial_schemas1,
      const std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>>& serial_schemas2);

  static butil::Status CompareSerialSchemaNonStrict(
      const std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>>& serial_schemas1,
      const std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>>& serial_schemas2,
      const ::google::protobuf::RepeatedField<int32_t>& group_by_columns,
      const ::google::protobuf::RepeatedPtrField<pb::store::AggregationOperator>& aggregation_operators);

  static std::any CloneColumn(const std::any& column, BaseSchema::Type type);

  static bool CoprocessorParamEmpty(const pb::store::Coprocessor& coprocessor);

  static void CloneCloneSerialSchemaVector(const std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>>& original,
                                           std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>>* copy);

  static void SortSerialSchemaVector(std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>>* schemas);

  static void DebugPbSchema(const google::protobuf::RepeatedPtrField<pb::store::Schema>& pb_schemas,
                            const std::string& name);

  static void DebugSerialSchema(const std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>>& serial_schemas,
                                const std::string& name);

  static void DebugGroupByOperators(
      const ::google::protobuf::RepeatedPtrField<pb::store::AggregationOperator>& aggregation_operators,
      const std::string& name);

  static void DebugInt32Index(const ::google::protobuf::RepeatedField<int32_t>& repeated_field_int32,
                              const std::string& name);

  static void DebugCoprocessor(const pb::store::Coprocessor& coprocessor);

  static void PrintColumn(const std::any& column, BaseSchema::Type type, const std::string& name);

  static void PrintGroupByKey(const std::string& key, const std::string& name);

};

}  // namespace dingodb

#endif  // DINGODB_COPROCESSOR_UTILS_H_  // NOLINT
