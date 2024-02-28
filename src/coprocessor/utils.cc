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

#include "coprocessor/utils.h"

#include <cxxabi.h>

#include <algorithm>
#include <any>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "common/helper.h"
#include "common/logging.h"
#include "fmt/core.h"
#include "proto/error.pb.h"
#include "proto/store.pb.h"
#include "serial/record_decoder.h"
#include "serial/record_encoder.h"
#include "serial/schema/base_schema.h"

namespace dingodb {

#undef COPROCESSOR_LOG
#undef COPROCESSOR_LOG_FOR_LAMBDA
#define COPROCESSOR_LOG DINGO_LOG(DEBUG)
#define COPROCESSOR_LOG_FOR_LAMBDA VLOG(DINGO_DEBUG) << "[" << __PRETTY_FUNCTION__ << "] "

template <typename PB_SCHEMA, typename CONSTRUCT, typename TYPE>
void SerialBaseSchemaConstructWrapper(const PB_SCHEMA& pb_schema, const CONSTRUCT& construct,
                                      std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>>* serial_schemas) {
  std::shared_ptr<DingoSchema<std::optional<TYPE>>> serial_schema =
      std::make_shared<DingoSchema<std::optional<TYPE>>>();
  construct(pb_schema, serial_schema);
  (*serial_schemas)->emplace_back(std::move(serial_schema));
}

template <typename SERIAL_SCHEMA, typename CLONE, typename TYPE>
void SerialBaseSchemaClonedWrapper(const SERIAL_SCHEMA& serial_schema, const CLONE& clone,
                                   SERIAL_SCHEMA* clone_serial_schema) {
  *clone_serial_schema = std::make_shared<DingoSchema<std::optional<TYPE>>>();
  std::shared_ptr<DingoSchema<std::optional<TYPE>>> type_clone_serial_schema =
      std::dynamic_pointer_cast<DingoSchema<std::optional<TYPE>>>(*clone_serial_schema);
  clone(serial_schema, type_clone_serial_schema);
}

template <typename SERIAL_SCHEMA, typename UPDATE, typename TYPE>
void SerialBaseSchemaUpdateIndexWrapper(size_t index, const UPDATE& update, SERIAL_SCHEMA* serial_schema) {
  std::shared_ptr<DingoSchema<std::optional<TYPE>>> type_serial_schema =
      std::dynamic_pointer_cast<DingoSchema<std::optional<TYPE>>>(*serial_schema);
  update(type_serial_schema, index);
}

template <typename SERIAL_SCHEMA, typename UPDATE, typename TYPE>
void SerialBaseSchemaUpdateKeyWrapper(bool is_key, const UPDATE& update, SERIAL_SCHEMA* serial_schema) {
  std::shared_ptr<DingoSchema<std::optional<TYPE>>> type_serial_schema =
      std::dynamic_pointer_cast<DingoSchema<std::optional<TYPE>>>(*serial_schema);
  update(type_serial_schema, is_key);
}

butil::Status Utils::CheckPbSchema(const google::protobuf::RepeatedPtrField<pb::common::Schema>& pb_schemas) {
  // if (pb_schemas.empty()) {
  //   std::string error_message = fmt::format("pb_schema empty. not support");
  //   DINGO_LOG(ERROR) << error_message;
  //   return butil::Status(pb::error::EILLEGAL_PARAMTETERS, error_message);
  // }

  size_t i = 0;
  for (const auto& schema : pb_schemas) {
    const auto& type = schema.type();
    // check null type ?
    if (type != pb::common::Schema::Type::Schema_Type_BOOL && type != pb::common::Schema::Type::Schema_Type_INTEGER &&
        type != pb::common::Schema::Type::Schema_Type_FLOAT && type != pb::common::Schema::Type::Schema_Type_LONG &&
        type != pb::common::Schema::Type::Schema_Type_DOUBLE && type != pb::common::Schema::Type::Schema_Type_STRING &&
        type != pb::common::Schema::Type::Schema_Type_BOOLLIST &&
        type != pb::common::Schema::Type::Schema_Type_INTEGERLIST &&
        type != pb::common::Schema::Type::Schema_Type_FLOATLIST &&
        type != pb::common::Schema::Type::Schema_Type_LONGLIST &&
        type != pb::common::Schema::Type::Schema_Type_DOUBLELIST &&
        type != pb::common::Schema::Type::Schema_Type_STRINGLIST) {
      std::string error_message = fmt::format("pb_schema invalid type : {}. not support", static_cast<int>(type));
      DINGO_LOG(ERROR) << error_message;
      return butil::Status(pb::error::EILLEGAL_PARAMTETERS, error_message);
    }

    // if (i != schema.index()) {
    //   std::string error_message = fmt::format("pb_schema invalid index : {} should be {}", schema.index(), i);
    //   DINGO_LOG(ERROR) << error_message;
    //   return butil::Status(pb::error::EILLEGAL_PARAMTETERS, error_message);
    // }
    i++;
  }

  return butil::Status();
}

butil::Status Utils::CheckSerialSchema(
    const std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>>& serial_schemas) {
  // if (serial_schemas && serial_schemas->empty()) {
  //   std::string error_message = fmt::format("serial_schemas empty. not support");
  //   DINGO_LOG(ERROR) << error_message;
  //   return butil::Status(pb::error::EILLEGAL_PARAMTETERS, error_message);
  // }

  size_t i = 0;
  for (const auto& schema : *serial_schemas) {
    const auto& type = schema->GetType();
    if (type != BaseSchema::Type::kBool && type != BaseSchema::Type::kInteger && type != BaseSchema::Type::kFloat &&
        type != BaseSchema::Type::kLong && type != BaseSchema::Type::kDouble && type != BaseSchema::Type::kString &&
        type != BaseSchema::Type::kBoolList && type != BaseSchema::Type::kIntegerList &&
        type != BaseSchema::Type::kFloatList && type != BaseSchema::Type::kLongList &&
        type != BaseSchema::Type::kDoubleList && type != BaseSchema::Type::kStringList) {
      std::string error_message = fmt::format("serial_schemas invalid type : {}. not support", static_cast<int>(type));
      DINGO_LOG(ERROR) << error_message;
      return butil::Status(pb::error::EILLEGAL_PARAMTETERS, error_message);
    }

    if (i != schema->GetIndex()) {
      std::string error_message = fmt::format("serial_schemas invalid index : {} should be {}", schema->GetIndex(), i);
      DINGO_LOG(ERROR) << error_message;
      return butil::Status(pb::error::EILLEGAL_PARAMTETERS, error_message);
    }
    i++;
  }

  return butil::Status();
}

butil::Status Utils::CheckSelection(const ::google::protobuf::RepeatedField<int32_t>& selection_columns,
                                    size_t original_schema_size) {
  // if (selection_columns.empty()) {
  //   std::string error_message = fmt::format("selection_columns empty. not support");
  //   DINGO_LOG(ERROR) << error_message;
  //   return butil::Status(pb::error::EILLEGAL_PARAMTETERS, error_message);
  // }

  for (const auto& index : selection_columns) {
    if (index < 0 || index >= static_cast<int>(original_schema_size)) {
      std::string error_message = fmt::format(
          "selection_columns index:{} < 0 || >= original_schema.size : {} . not support", index, original_schema_size);
      DINGO_LOG(ERROR) << error_message;
      return butil::Status(pb::error::EILLEGAL_PARAMTETERS, error_message);
    }
  }

  return butil::Status();
}

butil::Status Utils::CheckGroupByColumns(const ::google::protobuf::RepeatedField<int32_t>& group_by_columns,
                                         size_t selection_columns_size) {
  for (const auto& index : group_by_columns) {
    if (index < 0 || index >= selection_columns_size) {
      std::string error_message =
          fmt::format("group_by_columns index:{} < 0 || >= selection_columns.size : {} . not support", index,
                      selection_columns_size);
      DINGO_LOG(ERROR) << error_message;
      return butil::Status(pb::error::EILLEGAL_PARAMTETERS, error_message);
    }
  }

  return butil::Status();
}

butil::Status Utils::CheckGroupByOperators(
    const ::google::protobuf::RepeatedPtrField<pb::store::AggregationOperator>& aggregation_operators,
    size_t selection_columns_size) {
  size_t index = 0;
  for (const auto& elem : aggregation_operators) {
    if (elem.oper() != pb::store::AggregationType::SUM && elem.oper() != pb::store::AggregationType::COUNT &&
        elem.oper() != pb::store::AggregationType::COUNTWITHNULL && elem.oper() != pb::store::AggregationType::MAX &&
        elem.oper() != pb::store::AggregationType::MIN && elem.oper() != pb::store::AggregationType::SUM0) {
      std::string error_message =
          fmt::format("aggregation_operators index : {}  type : {}. not support", index, static_cast<int>(elem.oper()));
      DINGO_LOG(ERROR) << error_message;
      return butil::Status(pb::error::EILLEGAL_PARAMTETERS, error_message);
    }

    if (elem.index_of_column() >= static_cast<int>(selection_columns_size)) {
      if (elem.oper() != pb::store::AggregationType::COUNTWITHNULL) {
        std::string error_message =
            fmt::format("aggregation_operators index:{}  ({}>= selection_columns.size : {})  type:{}. not support",
                        index, selection_columns_size, elem.index_of_column(), static_cast<int>(elem.oper()));
        DINGO_LOG(ERROR) << error_message;
        return butil::Status(pb::error::EILLEGAL_PARAMTETERS, error_message);
      }
    } else if (elem.index_of_column() < 0) {
      if (elem.oper() != pb::store::AggregationType::COUNT &&
          elem.oper() != pb::store::AggregationType::COUNTWITHNULL) {
        std::string error_message = fmt::format("aggregation_operators index:{} ({} < 0) type:{}. not support", index,
                                                elem.index_of_column(), static_cast<int>(elem.oper()));
        DINGO_LOG(ERROR) << error_message;
        return butil::Status(pb::error::EILLEGAL_PARAMTETERS, error_message);
      }
    }

    index++;
  }

  return butil::Status();
}

butil::Status Utils::TransToSerialSchema(const google::protobuf::RepeatedPtrField<pb::common::Schema>& pb_schemas,
                                         std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>>* serial_schemas) {
  (*serial_schemas)->reserve(pb_schemas.size());

  size_t index = 0;
  for (const auto& pb_schema : pb_schemas) {
    auto serial_schema_construct_lambda = [](const auto& pb_schema, auto& serial_schema) {
      serial_schema->SetIsKey(pb_schema.is_key());
      serial_schema->SetAllowNull(pb_schema.is_nullable());
      serial_schema->SetIndex(pb_schema.index());
      serial_schema->SetName(pb_schema.name());
    };

    switch (pb_schema.type()) {
      case pb::common::Schema::Type::Schema_Type_BOOL: {
        SerialBaseSchemaConstructWrapper<decltype(pb_schema), decltype(serial_schema_construct_lambda), bool>(
            pb_schema, serial_schema_construct_lambda, serial_schemas);
        break;
      }
      case pb::common::Schema::Type::Schema_Type_INTEGER: {
        SerialBaseSchemaConstructWrapper<decltype(pb_schema), decltype(serial_schema_construct_lambda), int32_t>(
            pb_schema, serial_schema_construct_lambda, serial_schemas);

        break;
      }
      case pb::common::Schema::Type::Schema_Type_FLOAT: {
        SerialBaseSchemaConstructWrapper<decltype(pb_schema), decltype(serial_schema_construct_lambda), float>(
            pb_schema, serial_schema_construct_lambda, serial_schemas);

        break;
      }
      case pb::common::Schema::Type::Schema_Type_LONG: {
        SerialBaseSchemaConstructWrapper<decltype(pb_schema), decltype(serial_schema_construct_lambda), int64_t>(
            pb_schema, serial_schema_construct_lambda, serial_schemas);

        break;
      }
      case pb::common::Schema::Type::Schema_Type_DOUBLE: {
        SerialBaseSchemaConstructWrapper<decltype(pb_schema), decltype(serial_schema_construct_lambda), double>(
            pb_schema, serial_schema_construct_lambda, serial_schemas);

        break;
      }
      case pb::common::Schema::Type::Schema_Type_STRING: {
        SerialBaseSchemaConstructWrapper<decltype(pb_schema), decltype(serial_schema_construct_lambda),
                                         std::shared_ptr<std::string>>(pb_schema, serial_schema_construct_lambda,
                                                                       serial_schemas);

        break;
      }
      case pb::common::Schema::Type::Schema_Type_BOOLLIST: {
        SerialBaseSchemaConstructWrapper<decltype(pb_schema), decltype(serial_schema_construct_lambda),
                                         std::shared_ptr<std::vector<bool>>>(pb_schema, serial_schema_construct_lambda,
                                                                             serial_schemas);
        break;
      }
      case pb::common::Schema::Type::Schema_Type_INTEGERLIST: {
        SerialBaseSchemaConstructWrapper<decltype(pb_schema), decltype(serial_schema_construct_lambda),
                                         std::shared_ptr<std::vector<int32_t>>>(
            pb_schema, serial_schema_construct_lambda, serial_schemas);
        break;
      }
      case pb::common::Schema::Type::Schema_Type_FLOATLIST: {
        SerialBaseSchemaConstructWrapper<decltype(pb_schema), decltype(serial_schema_construct_lambda),
                                         std::shared_ptr<std::vector<float>>>(pb_schema, serial_schema_construct_lambda,
                                                                              serial_schemas);
        break;
      }
      case pb::common::Schema::Type::Schema_Type_LONGLIST: {
        SerialBaseSchemaConstructWrapper<decltype(pb_schema), decltype(serial_schema_construct_lambda),
                                         std::shared_ptr<std::vector<int64_t>>>(
            pb_schema, serial_schema_construct_lambda, serial_schemas);
        break;
      }
      case pb::common::Schema::Type::Schema_Type_DOUBLELIST: {
        SerialBaseSchemaConstructWrapper<decltype(pb_schema), decltype(serial_schema_construct_lambda),
                                         std::shared_ptr<std::vector<double>>>(
            pb_schema, serial_schema_construct_lambda, serial_schemas);
        break;
      }
      case pb::common::Schema::Type::Schema_Type_STRINGLIST: {
        SerialBaseSchemaConstructWrapper<decltype(pb_schema), decltype(serial_schema_construct_lambda),
                                         std::shared_ptr<std::vector<std::string>>>(
            pb_schema, serial_schema_construct_lambda, serial_schemas);
        break;
      }
      default: {
        std::string error_message = fmt::format("selection_columns index:{} < 0. not support", index);
        DINGO_LOG(ERROR) << error_message;
        return butil::Status(pb::error::EILLEGAL_PARAMTETERS, error_message);
      }
    }
    ++index;
  }

  return butil::Status();
}

std::shared_ptr<BaseSchema> Utils::CloneSerialSchema(const std::shared_ptr<BaseSchema>& serial_schema) {
  std::shared_ptr<BaseSchema> clone_serial_schema;
  if (serial_schema) {
    BaseSchema::Type type = serial_schema->GetType();
    auto serial_schema_clone_lambda = [](const auto& serial_schema, auto& clone_serial_schema) {
      clone_serial_schema->SetIsKey(serial_schema->IsKey());
      clone_serial_schema->SetAllowNull(serial_schema->AllowNull());
      clone_serial_schema->SetIndex(serial_schema->GetIndex());
      clone_serial_schema->SetName(serial_schema->GetName());
    };
    switch (type) {
      case BaseSchema::Type::kBool: {
        SerialBaseSchemaClonedWrapper<std::shared_ptr<BaseSchema>, decltype(serial_schema_clone_lambda), bool>(
            serial_schema, serial_schema_clone_lambda, &clone_serial_schema);

        break;
      }
      case BaseSchema::Type::kInteger: {
        SerialBaseSchemaClonedWrapper<std::shared_ptr<BaseSchema>, decltype(serial_schema_clone_lambda), int32_t>(
            serial_schema, serial_schema_clone_lambda, &clone_serial_schema);

        break;
      }
      case BaseSchema::Type::kFloat: {
        SerialBaseSchemaClonedWrapper<std::shared_ptr<BaseSchema>, decltype(serial_schema_clone_lambda), float>(
            serial_schema, serial_schema_clone_lambda, &clone_serial_schema);

        break;
      }
      case BaseSchema::Type::kLong: {
        SerialBaseSchemaClonedWrapper<std::shared_ptr<BaseSchema>, decltype(serial_schema_clone_lambda), int64_t>(
            serial_schema, serial_schema_clone_lambda, &clone_serial_schema);

        break;
      }
      case BaseSchema::Type::kDouble: {
        SerialBaseSchemaClonedWrapper<std::shared_ptr<BaseSchema>, decltype(serial_schema_clone_lambda), double>(
            serial_schema, serial_schema_clone_lambda, &clone_serial_schema);

        break;
      }
      case BaseSchema::Type::kString: {
        SerialBaseSchemaClonedWrapper<std::shared_ptr<BaseSchema>, decltype(serial_schema_clone_lambda),
                                      std::shared_ptr<std::string>>(serial_schema, serial_schema_clone_lambda,
                                                                    &clone_serial_schema);

        break;
      }
      case BaseSchema::Type::kBoolList: {
        SerialBaseSchemaClonedWrapper<std::shared_ptr<BaseSchema>, decltype(serial_schema_clone_lambda),
                                      std::shared_ptr<std::vector<bool>>>(serial_schema, serial_schema_clone_lambda,
                                                                          &clone_serial_schema);
        break;
      }
      case BaseSchema::Type::kIntegerList: {
        SerialBaseSchemaClonedWrapper<std::shared_ptr<BaseSchema>, decltype(serial_schema_clone_lambda),
                                      std::shared_ptr<std::vector<int32_t>>>(serial_schema, serial_schema_clone_lambda,
                                                                             &clone_serial_schema);
        break;
      }
      case BaseSchema::Type::kFloatList: {
        SerialBaseSchemaClonedWrapper<std::shared_ptr<BaseSchema>, decltype(serial_schema_clone_lambda),
                                      std::shared_ptr<std::vector<float>>>(serial_schema, serial_schema_clone_lambda,
                                                                           &clone_serial_schema);
        break;
      }
      case BaseSchema::Type::kLongList: {
        SerialBaseSchemaClonedWrapper<std::shared_ptr<BaseSchema>, decltype(serial_schema_clone_lambda),
                                      std::shared_ptr<std::vector<int64_t>>>(serial_schema, serial_schema_clone_lambda,
                                                                             &clone_serial_schema);
        break;
      }
      case BaseSchema::Type::kDoubleList: {
        SerialBaseSchemaClonedWrapper<std::shared_ptr<BaseSchema>, decltype(serial_schema_clone_lambda),
                                      std::shared_ptr<std::vector<double>>>(serial_schema, serial_schema_clone_lambda,
                                                                            &clone_serial_schema);
        break;
      }
      case BaseSchema::Type::kStringList: {
        SerialBaseSchemaClonedWrapper<std::shared_ptr<BaseSchema>, decltype(serial_schema_clone_lambda),
                                      std::shared_ptr<std::vector<std::string>>>(
            serial_schema, serial_schema_clone_lambda, &clone_serial_schema);
        break;
      }
      default: {
        return clone_serial_schema;
      }
    }
  }
  return clone_serial_schema;
}

butil::Status Utils::CreateSerialSchema(
    const std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>>& old_serial_schemas,
    const ::google::protobuf::RepeatedField<int32_t>& new_columns,
    std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>>* new_serial_schemas) {
  if (old_serial_schemas && !old_serial_schemas->empty() && !new_columns.empty()) {
    (*new_serial_schemas)->clear();
    (*new_serial_schemas)->reserve(new_columns.size());

    for (const auto& index : new_columns) {
      std::shared_ptr<BaseSchema> original_serial_schema = FindSerialSchemaVector(old_serial_schemas, index);
      if (!original_serial_schema) {
        std::string error_message = fmt::format("FindSerialSchemaVector failed index : {}", index);
        DINGO_LOG(ERROR) << error_message;
        return butil::Status(pb::error::EILLEGAL_PARAMTETERS, error_message);
      }
      // const std::shared_ptr<BaseSchema>& original_serial_schema = (*old_serial_schemas)[index];
      std::shared_ptr<BaseSchema> clone_serial_schema = CloneSerialSchema(original_serial_schema);
      (**new_serial_schemas).emplace_back(std::move(clone_serial_schema));
    }
  }

  return butil::Status();
}

butil::Status Utils::CreateSerialSchema(
    const std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>>& old_serial_schemas,
    const ::google::protobuf::RepeatedField<int32_t>& new_columns, const std::vector<int>& selection_columns,
    std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>>* new_serial_schemas) {
  if (old_serial_schemas && !old_serial_schemas->empty() && !new_columns.empty()) {
    (*new_serial_schemas)->clear();
    (*new_serial_schemas)->reserve(new_columns.size());

    for (const auto& index : new_columns) {
      DINGO_LOG(DEBUG) << "CreateSerialSchema index:" << index;
      int index_selection = selection_columns[index];
      DINGO_LOG(DEBUG) << "index_selection:" << index_selection;
      std::shared_ptr<BaseSchema> original_serial_schema = (*old_serial_schemas)[index_selection];
      if (!original_serial_schema) {
        std::string error_message = fmt::format("FindSerialSchemaVector failed index : {}", index);
        DINGO_LOG(ERROR) << error_message;
        return butil::Status(pb::error::EILLEGAL_PARAMTETERS, error_message);
      }
      // const std::shared_ptr<BaseSchema>& original_serial_schema = (*old_serial_schemas)[index];
      std::shared_ptr<BaseSchema> clone_serial_schema = CloneSerialSchema(original_serial_schema);
      (**new_serial_schemas).emplace_back(std::move(clone_serial_schema));
    }
  }

  return butil::Status();
}
butil::Status Utils::CreateSelectionSchema(
    const std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>>& old_serial_schemas,
    const std::vector<int>& new_columns,
    std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>>* new_serial_schemas) {
  if (old_serial_schemas && !old_serial_schemas->empty() && !new_columns.empty()) {
    (*new_serial_schemas)->clear();
    (*new_serial_schemas)->reserve(new_columns.size());

    for (const auto& index : new_columns) {
      std::shared_ptr<BaseSchema> original_serial_schema = FindSerialSchemaVector(old_serial_schemas, index);
      if (!original_serial_schema) {
        std::string error_message = fmt::format("FindSerialSchemaVector failed index : {}", index);
        DINGO_LOG(ERROR) << error_message;
        return butil::Status(pb::error::EILLEGAL_PARAMTETERS, error_message);
      }
      // const std::shared_ptr<BaseSchema>& original_serial_schema = (*old_serial_schemas)[index];
      std::shared_ptr<BaseSchema> clone_serial_schema = CloneSerialSchema(original_serial_schema);
      (**new_serial_schemas).emplace_back(std::move(clone_serial_schema));
    }
  }

  return butil::Status();
}

butil::Status Utils::UpdateSerialSchemaIndex(
    std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>>* serial_schemas) {
  if (serial_schemas && *serial_schemas && !(*serial_schemas)->empty()) {
    size_t i = 0;
    for (auto& serial_schema : **serial_schemas) {
      BaseSchema::Type type = serial_schema->GetType();
      auto serial_schema_update_index_lambda = [](auto& serial_schema, size_t index) {
        serial_schema->SetIndex(static_cast<int>(index));
      };

      switch (type) {
        case BaseSchema::Type::kBool: {
          SerialBaseSchemaUpdateIndexWrapper<std::shared_ptr<BaseSchema>, decltype(serial_schema_update_index_lambda),
                                             bool>(i, serial_schema_update_index_lambda, &serial_schema);
          break;
        }
        case BaseSchema::Type::kInteger: {
          SerialBaseSchemaUpdateIndexWrapper<std::shared_ptr<BaseSchema>, decltype(serial_schema_update_index_lambda),
                                             int32_t>(i, serial_schema_update_index_lambda, &serial_schema);

          break;
        }
        case BaseSchema::Type::kFloat: {
          SerialBaseSchemaUpdateIndexWrapper<std::shared_ptr<BaseSchema>, decltype(serial_schema_update_index_lambda),
                                             float>(i, serial_schema_update_index_lambda, &serial_schema);

          break;
        }
        case BaseSchema::Type::kLong: {
          SerialBaseSchemaUpdateIndexWrapper<std::shared_ptr<BaseSchema>, decltype(serial_schema_update_index_lambda),
                                             int64_t>(i, serial_schema_update_index_lambda, &serial_schema);
          break;
        }
        case BaseSchema::Type::kDouble: {
          SerialBaseSchemaUpdateIndexWrapper<std::shared_ptr<BaseSchema>, decltype(serial_schema_update_index_lambda),
                                             double>(i, serial_schema_update_index_lambda, &serial_schema);
          break;
        }
        case BaseSchema::Type::kString: {
          SerialBaseSchemaUpdateIndexWrapper<std::shared_ptr<BaseSchema>, decltype(serial_schema_update_index_lambda),
                                             std::shared_ptr<std::string>>(i, serial_schema_update_index_lambda,
                                                                           &serial_schema);
          break;
        }
        case BaseSchema::Type::kBoolList: {
          SerialBaseSchemaUpdateIndexWrapper<std::shared_ptr<BaseSchema>, decltype(serial_schema_update_index_lambda),
                                             std::shared_ptr<std::vector<bool>>>(i, serial_schema_update_index_lambda,
                                                                                 &serial_schema);
          break;
        }
        case BaseSchema::Type::kIntegerList: {
          SerialBaseSchemaUpdateIndexWrapper<std::shared_ptr<BaseSchema>, decltype(serial_schema_update_index_lambda),
                                             std::shared_ptr<std::vector<int32_t>>>(
              i, serial_schema_update_index_lambda, &serial_schema);
          break;
        }
        case BaseSchema::Type::kFloatList: {
          SerialBaseSchemaUpdateIndexWrapper<std::shared_ptr<BaseSchema>, decltype(serial_schema_update_index_lambda),
                                             std::shared_ptr<std::vector<float>>>(i, serial_schema_update_index_lambda,
                                                                                  &serial_schema);
          break;
        }
        case BaseSchema::Type::kLongList: {
          SerialBaseSchemaUpdateIndexWrapper<std::shared_ptr<BaseSchema>, decltype(serial_schema_update_index_lambda),
                                             std::shared_ptr<std::vector<int64_t>>>(
              i, serial_schema_update_index_lambda, &serial_schema);
          break;
        }
        case BaseSchema::Type::kDoubleList: {
          SerialBaseSchemaUpdateIndexWrapper<std::shared_ptr<BaseSchema>, decltype(serial_schema_update_index_lambda),
                                             std::shared_ptr<std::vector<double>>>(i, serial_schema_update_index_lambda,
                                                                                   &serial_schema);
          break;
        }
        case BaseSchema::Type::kStringList: {
          SerialBaseSchemaUpdateIndexWrapper<std::shared_ptr<BaseSchema>, decltype(serial_schema_update_index_lambda),
                                             std::shared_ptr<std::vector<std::string>>>(
              i, serial_schema_update_index_lambda, &serial_schema);
          break;
        }
        default: {
          std::string error_message = fmt::format("index:{} invalid BaseSchema type", i, static_cast<int>(type));
          DINGO_LOG(ERROR) << error_message;
          return butil::Status(pb::error::EILLEGAL_PARAMTETERS, error_message);
        }
      }

      i++;
    }
  }

  return butil::Status();
}

butil::Status Utils::UpdateSerialSchemaKey(const std::vector<bool>& keys,
                                           std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>>* serial_schemas) {
  if (serial_schemas && *serial_schemas && !(*serial_schemas)->empty() && (keys.size() == (*serial_schemas)->size())) {
    size_t i = 0;
    for (auto& serial_schema : **serial_schemas) {
      BaseSchema::Type type = serial_schema->GetType();
      auto serial_schema_update_key_lambda = [](auto& serial_schema, bool is_key) { serial_schema->SetIsKey(is_key); };

      switch (type) {
        case BaseSchema::Type::kBool: {
          SerialBaseSchemaUpdateKeyWrapper<std::shared_ptr<BaseSchema>, decltype(serial_schema_update_key_lambda),
                                           bool>(keys[i], serial_schema_update_key_lambda, &serial_schema);
          break;
        }
        case BaseSchema::Type::kInteger: {
          SerialBaseSchemaUpdateKeyWrapper<std::shared_ptr<BaseSchema>, decltype(serial_schema_update_key_lambda),
                                           int32_t>(keys[i], serial_schema_update_key_lambda, &serial_schema);

          break;
        }
        case BaseSchema::Type::kFloat: {
          SerialBaseSchemaUpdateKeyWrapper<std::shared_ptr<BaseSchema>, decltype(serial_schema_update_key_lambda),
                                           float>(keys[i], serial_schema_update_key_lambda, &serial_schema);

          break;
        }
        case BaseSchema::Type::kLong: {
          SerialBaseSchemaUpdateKeyWrapper<std::shared_ptr<BaseSchema>, decltype(serial_schema_update_key_lambda),
                                           int64_t>(keys[i], serial_schema_update_key_lambda, &serial_schema);
          break;
        }
        case BaseSchema::Type::kDouble: {
          SerialBaseSchemaUpdateKeyWrapper<std::shared_ptr<BaseSchema>, decltype(serial_schema_update_key_lambda),
                                           double>(keys[i], serial_schema_update_key_lambda, &serial_schema);
          break;
        }
        case BaseSchema::Type::kString: {
          SerialBaseSchemaUpdateKeyWrapper<std::shared_ptr<BaseSchema>, decltype(serial_schema_update_key_lambda),
                                           std::shared_ptr<std::string>>(keys[i], serial_schema_update_key_lambda,
                                                                         &serial_schema);
          break;
        }
        case BaseSchema::Type::kBoolList: {
          SerialBaseSchemaUpdateKeyWrapper<std::shared_ptr<BaseSchema>, decltype(serial_schema_update_key_lambda),
                                           std::shared_ptr<std::vector<bool>>>(keys[i], serial_schema_update_key_lambda,
                                                                               &serial_schema);
          break;
        }
        case BaseSchema::Type::kIntegerList: {
          SerialBaseSchemaUpdateKeyWrapper<std::shared_ptr<BaseSchema>, decltype(serial_schema_update_key_lambda),
                                           std::shared_ptr<std::vector<int32_t>>>(
              keys[i], serial_schema_update_key_lambda, &serial_schema);
          break;
        }
        case BaseSchema::Type::kFloatList: {
          SerialBaseSchemaUpdateKeyWrapper<std::shared_ptr<BaseSchema>, decltype(serial_schema_update_key_lambda),
                                           std::shared_ptr<std::vector<float>>>(
              keys[i], serial_schema_update_key_lambda, &serial_schema);
          break;
        }
        case BaseSchema::Type::kLongList: {
          SerialBaseSchemaUpdateKeyWrapper<std::shared_ptr<BaseSchema>, decltype(serial_schema_update_key_lambda),
                                           std::shared_ptr<std::vector<int64_t>>>(
              keys[i], serial_schema_update_key_lambda, &serial_schema);
          break;
        }
        case BaseSchema::Type::kDoubleList: {
          SerialBaseSchemaUpdateKeyWrapper<std::shared_ptr<BaseSchema>, decltype(serial_schema_update_key_lambda),
                                           std::shared_ptr<std::vector<double>>>(
              keys[i], serial_schema_update_key_lambda, &serial_schema);
          break;
        }
        case BaseSchema::Type::kStringList: {
          SerialBaseSchemaUpdateKeyWrapper<std::shared_ptr<BaseSchema>, decltype(serial_schema_update_key_lambda),
                                           std::shared_ptr<std::vector<std::string>>>(
              keys[i], serial_schema_update_key_lambda, &serial_schema);
          break;
        }
        default: {
          std::string error_message = fmt::format("index:{} invalid BaseSchema type", i, static_cast<int>(type));
          DINGO_LOG(ERROR) << error_message;
          return butil::Status(pb::error::EILLEGAL_PARAMTETERS, error_message);
        }
      }

      i++;
    }
  }

  return butil::Status();
}

butil::Status Utils::JoinSerialSchema(
    const std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>>& old_serial_schemas1,
    const std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>>& old_serial_schemas2,
    std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>>* new_serial_schemas) {
  if ((old_serial_schemas1 && !old_serial_schemas1->empty()) ||
      (old_serial_schemas2 && !old_serial_schemas2->empty())) {
    *new_serial_schemas = std::make_shared<std::vector<std::shared_ptr<BaseSchema>>>();
    (*new_serial_schemas)->clear();
    (*new_serial_schemas)
        ->reserve((old_serial_schemas1 && !old_serial_schemas1->empty() ? old_serial_schemas1->size() : 0) +
                  (old_serial_schemas2 && !old_serial_schemas2->empty() ? old_serial_schemas2->size() : 0));

    if (old_serial_schemas1 && !old_serial_schemas1->empty()) {
      for (size_t i = 0; i < old_serial_schemas1->size(); i++) {
        const std::shared_ptr<BaseSchema>& old_serial_schema = (*old_serial_schemas1)[i];
        std::shared_ptr<BaseSchema> clone_serial_schema = CloneSerialSchema(old_serial_schema);
        (**new_serial_schemas).emplace_back(std::move(clone_serial_schema));
      }
    }

    if (old_serial_schemas2 && !old_serial_schemas2->empty()) {
      for (size_t i = 0; i < old_serial_schemas2->size(); i++) {
        const std::shared_ptr<BaseSchema>& old_serial_schema = (*old_serial_schemas2)[i];
        std::shared_ptr<BaseSchema> clone_serial_schema = CloneSerialSchema(old_serial_schema);
        (**new_serial_schemas).emplace_back(std::move(clone_serial_schema));
      }
    }
  }
  return butil::Status();
}

butil::Status Utils::CompareSerialSchemaStrict(
    const std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>>& serial_schemas1,
    const std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>>& serial_schemas2) {
  if (!serial_schemas1 && !serial_schemas2) {
    return butil::Status();
  } else if (!serial_schemas1 && serial_schemas2) {
    std::string error_message =
        fmt::format("CompareSerialSchemaStrict failed  serial_schema1 invalid unequal serial_schema2 valid ");
    DINGO_LOG(ERROR) << error_message;
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, error_message);
  } else if (serial_schemas1 && !serial_schemas2) {
    std::string error_message =
        fmt::format("CompareSerialSchemaStrict failed  serial_schema1 valid  unequal serial_schema2 invalid");
    DINGO_LOG(ERROR) << error_message;
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, error_message);
  }

  // if (serial_schemas1->size() != serial_schemas2->size()) {
  //   std::string error_message =
  //       fmt::format("CompareSerialSchemaStrict failed  serial_schema1 size: {} unequal serial_schema2 size : {}",
  //                   serial_schemas1->size(), serial_schemas2->size());
  //   DINGO_LOG(ERROR) << error_message;
  //   return butil::Status(pb::error::EILLEGAL_PARAMTETERS, error_message);
  // }

  for (size_t i = 0; i < serial_schemas1->size() && i < serial_schemas2->size(); i++) {
    const auto& serial_schema1 = (*serial_schemas1)[i];
    const auto& serial_schema2 = (*serial_schemas2)[i];
    if (serial_schema1->GetType() != serial_schema2->GetType()) {
      std::string error_message = fmt::format(
          "CompareSerialSchemaStrict failed index : {}  serial_schema1 type: {} unequal serial_schema2 type : {}", i,
          BaseSchema::GetTypeString(serial_schema1->GetType()), BaseSchema::GetTypeString(serial_schema2->GetType()));
      DINGO_LOG(ERROR) << error_message;
      return butil::Status(pb::error::EILLEGAL_PARAMTETERS, error_message);
    }
  }

  return butil::Status();
}

butil::Status Utils::CompareSerialSchemaNonStrict(
    const std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>>& serial_schemas1,
    const std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>>& serial_schemas2,
    const ::google::protobuf::RepeatedField<int32_t>& group_by_columns,
    const ::google::protobuf::RepeatedPtrField<pb::store::AggregationOperator>& aggregation_operators) {
  if (serial_schemas1->size() != serial_schemas2->size()) {
    std::string error_message =
        fmt::format("CompareSerialSchemaNonStrict failed  serial_schema1 size: {} unequal serial_schema2 size : {}",
                    serial_schemas1->size(), serial_schemas2->size());
    DINGO_LOG(ERROR) << error_message;
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, error_message);
  }

  size_t i = 0;

  for (; i < serial_schemas1->size() && i < serial_schemas2->size() && i < group_by_columns.size(); i++) {
    const auto& serial_schema1 = (*serial_schemas1)[i];
    const auto& serial_schema2 = (*serial_schemas2)[i];
    if (serial_schema1->GetType() != serial_schema2->GetType()) {
      std::string error_message = fmt::format(
          "enable CompareSerialSchemaNonStrict failed group by key  index : {}  serial_schema1 type: {} unequal "
          "serial_schema2 type : {}",
          i, BaseSchema::GetTypeString(serial_schema1->GetType()),
          BaseSchema::GetTypeString(serial_schema2->GetType()));
      DINGO_LOG(ERROR) << error_message;
      return butil::Status(pb::error::EILLEGAL_PARAMTETERS, error_message);
    }
  }

  size_t j = 0;

  for (; i < serial_schemas1->size() && i < serial_schemas2->size() && j < aggregation_operators.size(); i++, j++) {
    const auto& serial_schema1 = (*serial_schemas1)[i];
    const auto& serial_schema2 = (*serial_schemas2)[i];
    if (serial_schema1->GetType() != serial_schema2->GetType()) {
      const auto& aggregation_operator = aggregation_operators[j];
      // aggregation_operator.oper();
      if ((aggregation_operator.oper() == pb::store::AggregationType::COUNT ||
           aggregation_operator.oper() == pb::store::AggregationType::COUNTWITHNULL) &&
          BaseSchema::Type::kLong == serial_schema1->GetType()) {
        continue;
      } else {
        std::string error_message = fmt::format(
            "enable CompareSerialSchemaNonStrict failed index : {} aggregation index : {}  serial_schema1 type: {} "
            "unequal "
            "serial_schema2 type : {}",
            i, j, BaseSchema::GetTypeString(serial_schema1->GetType()),
            BaseSchema::GetTypeString(serial_schema2->GetType()));
        DINGO_LOG(ERROR) << error_message;
        return butil::Status(pb::error::EILLEGAL_PARAMTETERS, error_message);
      }
    }
  }

  return butil::Status();
}

std::any Utils::CloneColumn(const std::any& column, BaseSchema::Type type) {
  switch (type) {
    case BaseSchema::Type::kBool: {
      try {
        const std::optional<bool>& value = std::any_cast<std::optional<bool>>(column);
        if (value.has_value()) {
          return std::optional<bool>(value.value());
        } else {
          return std::optional<bool>(std::nullopt);
        }
      } catch (const std::bad_any_cast& bad) {
        DINGO_LOG(ERROR) << fmt::format("{}  any_cast std::optional<bool> failed", bad.what());
        return {};
      }
    }
    case BaseSchema::Type::kInteger: {
      try {
        const std::optional<int32_t>& value = std::any_cast<std::optional<int32_t>>(column);
        if (value.has_value()) {
          return std::optional<int32_t>(value.value());
        } else {
          return std::optional<int32_t>(std::nullopt);
        }
      } catch (const std::bad_any_cast& bad) {
        DINGO_LOG(ERROR) << fmt::format("{}  any_cast std::optional<int32_t> failed", bad.what());
        return {};
      }
    }
    case BaseSchema::Type::kFloat: {
      try {
        const std::optional<float>& value = std::any_cast<std::optional<float>>(column);
        if (value.has_value()) {
          return std::optional<float>(value.value());
        } else {
          return std::optional<float>(std::nullopt);
        }
      } catch (const std::bad_any_cast& bad) {
        DINGO_LOG(ERROR) << fmt::format("{}  any_cast std::optional<float> failed", bad.what());
        return {};
      }
    }
    case BaseSchema::Type::kLong: {
      try {
        const std::optional<int64_t>& value = std::any_cast<std::optional<int64_t>>(column);
        if (value.has_value()) {
          return std::optional<int64_t>(value.value());
        } else {
          return std::optional<int64_t>(std::nullopt);
        }
      } catch (const std::bad_any_cast& bad) {
        DINGO_LOG(ERROR) << fmt::format("{}  any_cast std::optional<int64_t> failed", bad.what());
        return {};
      }
    }
    case BaseSchema::Type::kDouble: {
      try {
        const std::optional<double>& value = std::any_cast<std::optional<double>>(column);
        if (value.has_value()) {
          return std::optional<double>(value.value());
        } else {
          return std::optional<double>(std::nullopt);
        }
      } catch (const std::bad_any_cast& bad) {
        DINGO_LOG(ERROR) << fmt::format("{}  any_cast std::optional<double> failed", bad.what());
        return {};
      }
    }
    case BaseSchema::Type::kString: {
      try {
        const std::optional<std::shared_ptr<std::string>>& value =
            std::any_cast<std::optional<std::shared_ptr<std::string>>>(column);
        if (value.has_value()) {
          // return std::optional<std::shared_ptr<std::string>>(new std::string(*value.value()));
          return std::optional<std::shared_ptr<std::string>>(value.value());
        } else {
          return std::optional<std::shared_ptr<std::string>>(std::nullopt);
        }
      } catch (const std::bad_any_cast& bad) {
        DINGO_LOG(ERROR) << fmt::format("{}  any_cast std::optional<std::shared_ptr<std::string>> failed", bad.what());
        return {};
      }
    }
    case BaseSchema::Type::kBoolList: {
      try {
        const std::optional<std::shared_ptr<std::vector<bool>>>& value =
            std::any_cast<std::optional<std::shared_ptr<std::vector<bool>>>>(column);
        if (value.has_value()) {
          return std::optional<std::shared_ptr<std::vector<bool>>>(value.value());
        } else {
          return std::optional<std::shared_ptr<std::vector<bool>>>(std::nullopt);
        }
      } catch (const std::bad_any_cast& bad) {
        DINGO_LOG(ERROR) << fmt::format("{}  any_cast std::optional<std::shared_ptr<std::vector<bool>>> failed",
                                        bad.what());
        return {};
      }
    }
    case BaseSchema::Type::kIntegerList: {
      try {
        const std::optional<std::shared_ptr<std::vector<int32_t>>>& value =
            std::any_cast<std::optional<std::shared_ptr<std::vector<int32_t>>>>(column);
        if (value.has_value()) {
          return std::optional<std::shared_ptr<std::vector<int32_t>>>(value.value());
        } else {
          return std::optional<std::shared_ptr<std::vector<int32_t>>>(std::nullopt);
        }
      } catch (const std::bad_any_cast& bad) {
        DINGO_LOG(ERROR) << fmt::format("{}  any_cast std::optional<std::shared_ptr<std::vector<int32_t>>> failed",
                                        bad.what());
        return {};
      }
    }
    case BaseSchema::Type::kFloatList: {
      try {
        const std::optional<std::shared_ptr<std::vector<float>>>& value =
            std::any_cast<std::optional<std::shared_ptr<std::vector<float>>>>(column);
        if (value.has_value()) {
          return std::optional<std::shared_ptr<std::vector<float>>>(value.value());
        } else {
          return std::optional<std::shared_ptr<std::vector<float>>>(std::nullopt);
        }
      } catch (const std::bad_any_cast& bad) {
        DINGO_LOG(ERROR) << fmt::format("{}  any_cast std::optional<std::shared_ptr<std::vector<float>>> failed",
                                        bad.what());
        return {};
      }
    }
    case BaseSchema::Type::kLongList: {
      try {
        const std::optional<std::shared_ptr<std::vector<int64_t>>>& value =
            std::any_cast<std::optional<std::shared_ptr<std::vector<int64_t>>>>(column);
        if (value.has_value()) {
          return std::optional<std::shared_ptr<std::vector<int64_t>>>(value.value());
        } else {
          return std::optional<std::shared_ptr<std::vector<int64_t>>>(std::nullopt);
        }
      } catch (const std::bad_any_cast& bad) {
        DINGO_LOG(ERROR) << fmt::format("{}  any_cast std::optional<std::shared_ptr<std::vector<int64_t>>> failed",
                                        bad.what());
        return {};
      }
    }
    case BaseSchema::Type::kDoubleList: {
      try {
        const std::optional<std::shared_ptr<std::vector<double>>>& value =
            std::any_cast<std::optional<std::shared_ptr<std::vector<double>>>>(column);
        if (value.has_value()) {
          return std::optional<std::shared_ptr<std::vector<double>>>(value.value());
        } else {
          return std::optional<std::shared_ptr<std::vector<double>>>(std::nullopt);
        }
      } catch (const std::bad_any_cast& bad) {
        DINGO_LOG(ERROR) << fmt::format("{}  any_cast std::optional<std::shared_ptr<std::vector<double>>> failed",
                                        bad.what());
        return {};
      }
    }
    case BaseSchema::Type::kStringList: {
      try {
        const std::optional<std::shared_ptr<std::vector<std::string>>>& value =
            std::any_cast<std::optional<std::shared_ptr<std::vector<std::string>>>>(column);
        if (value.has_value()) {
          return std::optional<std::shared_ptr<std::vector<std::string>>>(value.value());
        } else {
          return std::optional<std::shared_ptr<std::vector<std::string>>>(std::nullopt);
        }
      } catch (const std::bad_any_cast& bad) {
        DINGO_LOG(ERROR) << fmt::format("{}  any_cast std::optional<std::shared_ptr<std::vector<std::string>>> failed",
                                        bad.what());
        return {};
      }
    }
    default: {
      std::string error_message = fmt::format("CloneColumn unsupported type  {}", static_cast<int>(type));
      DINGO_LOG(ERROR) << error_message;
      return {};
    }
  }

  return {};
}

bool Utils::CoprocessorParamEmpty(const pb::store::Coprocessor& coprocessor) {
  if (0 != coprocessor.schema_version()) {
    return false;
  }

  if (!coprocessor.original_schema().schema().empty()) {
    return false;
  }

  if (0 != coprocessor.original_schema().common_id()) {
    return false;
  }

  if (!coprocessor.result_schema().schema().empty()) {
    return false;
  }

  if (0 != coprocessor.result_schema().common_id()) {
    return false;
  }

  if (!coprocessor.selection_columns().empty()) {
    return false;
  }

  if (!coprocessor.expression().empty()) {
    return false;
  }

  if (!coprocessor.group_by_columns().empty()) {
    return false;
  }

  if (!coprocessor.aggregation_operators().empty()) {
    return false;
  }

  return true;
}

void Utils::CloneCloneSerialSchemaVector(const std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>>& original,
                                         std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>>* copy) {
  (*copy)->reserve(original->size());
  for (const auto& serial_schema : *original) {
    (*copy)->emplace_back(Utils::CloneSerialSchema(serial_schema));
  }
}

void Utils::SortSerialSchemaVector(std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>>* schemas) {
  // sort by index
  std::sort((*schemas)->begin(), (*schemas)->end(),
            [](std::shared_ptr<BaseSchema>& bs1, std::shared_ptr<BaseSchema>& bs2) {
              return bs1->GetIndex() < bs2->GetIndex();
            });
}

std::shared_ptr<BaseSchema> Utils::FindSerialSchemaVector(
    const std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>>& schemas, int index) {
  for (const auto& schema : *schemas) {
    if (index == schema->GetIndex()) {
      return schema;
    }
  }
  return {};
}

void Utils::DebugPbSchema(const google::protobuf::RepeatedPtrField<pb::common::Schema>& pb_schemas,
                          const std::string& name) {
  COPROCESSOR_LOG
      << "***************************DebugPbSchema Start*****************************************************";
  COPROCESSOR_LOG << name;

  size_t i = 0;
  for (const auto& schema : pb_schemas) {
    std::stringstream ss;

    const auto& type = schema.type();

    ss << "[" << i << "] ";

    ss << "Schema_Type : ";

    switch (type) {
      case pb::common::Schema_Type_BOOL:
        ss << "Schema_Type_BOOL";
        break;
      case pb::common::Schema_Type_INTEGER:
        ss << "Schema_Type_INTEGER";
        break;
      case pb::common::Schema_Type_FLOAT:
        ss << "Schema_Type_FLOAT";
        break;
      case pb::common::Schema_Type_LONG:
        ss << "Schema_Type_LONG";
        break;
      case pb::common::Schema_Type_DOUBLE:
        ss << "Schema_Type_DOUBLE";
        break;
      case pb::common::Schema_Type_STRING:
        ss << "Schema_Type_STRING";
        break;
      case pb::common::Schema_Type_BOOLLIST:
        ss << "Schema_Type_BOOLLIST";
        break;
      case pb::common::Schema_Type_INTEGERLIST:
        ss << "Schema_Type_INTEGERLIST";
        break;
      case pb::common::Schema_Type_FLOATLIST:
        ss << "Schema_Type_FLOATLIST";
        break;
      case pb::common::Schema_Type_LONGLIST:
        ss << "Schema_Type_LONGLIST";
        break;
      case pb::common::Schema_Type_DOUBLELIST:
        ss << "Schema_Type_DOUBLELIST";
        break;
      case pb::common::Schema_Type_STRINGLIST:
        ss << "Schema_Type_STRINGLIST";
        break;
      case pb::common::Schema_Type_Schema_Type_INT_MIN_SENTINEL_DO_NOT_USE_:
      case pb::common::Schema_Type_Schema_Type_INT_MAX_SENTINEL_DO_NOT_USE_:
        break;
    }

    ss << "\tis_key : " << (schema.is_key() ? "true" : "false");
    ss << "\tis_nullable : " << (schema.is_nullable() ? "true" : "false");
    ss << "\tindex : " << (schema.index());
    ss << "\tname : " << (schema.name());

    COPROCESSOR_LOG << ss.str() << "\n";
    i++;
  }

  COPROCESSOR_LOG << "***************************DebugPbSchema End*****************************************************"
                  << "\n";
}

void Utils::DebugSerialSchema(const std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>>& serial_schemas,
                              const std::string& name) {
  COPROCESSOR_LOG
      << "***************************DebugSerialSchema Start*****************************************************";
  COPROCESSOR_LOG << name;

  size_t i = 0;

  if (serial_schemas) {
    for (const auto& schema : *serial_schemas) {
      std::stringstream ss;

      const auto& type = schema->GetType();

      ss << "[" << i << "] ";
      ss << "Schema_Type : ";

      switch (type) {
        case BaseSchema::kBool:
          ss << "BaseSchema::kBool";
          break;
        case BaseSchema::kInteger:
          ss << "BaseSchema::kInteger";
          break;
        case BaseSchema::kFloat:
          ss << "BaseSchema::kFloat";
          break;
        case BaseSchema::kLong:
          ss << "BaseSchema::kLong";
          break;
        case BaseSchema::kDouble:
          ss << "BaseSchema::kDouble";
          break;
        case BaseSchema::kString:
          ss << "BaseSchema::kString";
          break;
        case BaseSchema::kBoolList:
          ss << "BaseSchema::kBoolList";
          break;
        case BaseSchema::kIntegerList:
          ss << "BaseSchema::kIntegerList";
          break;
        case BaseSchema::kFloatList:
          ss << "BaseSchema::kFloatList";
          break;
        case BaseSchema::kLongList:
          ss << "BaseSchema::kLongList";
          break;
        case BaseSchema::kDoubleList:
          ss << "BaseSchema::kDoubleList";
          break;
        case BaseSchema::kStringList:
          ss << "BaseSchema::kStringList";
          break;
      }

      ss << "\tIsKey : " << (schema->IsKey() ? "true" : "false");
      ss << "\tAllowNull : " << (schema->AllowNull() ? "true" : "false");
      ss << "\tIndex : " << (schema->GetIndex());
      ss << "\tName : " << (schema->GetName());

      COPROCESSOR_LOG << ss.str() << "\n";
      i++;
    }
  }

  COPROCESSOR_LOG
      << "***************************DebugSerialSchema End*****************************************************";
}

void Utils::DebugGroupByOperators(
    const ::google::protobuf::RepeatedPtrField<pb::store::AggregationOperator>& aggregation_operators,
    const std::string& name) {
  COPROCESSOR_LOG
      << "***************************DebugGroupByOperators Start*****************************************************";
  COPROCESSOR_LOG << name;

  size_t i = 0;

  for (const auto& aggregation_operator : aggregation_operators) {
    std::stringstream ss;

    const auto& oper = aggregation_operator.oper();

    ss << "[" << i << "] ";

    ss << "Oper_Type : ";

    switch (oper) {
      case pb::store::AGGREGATION_NONE:
        ss << "AGGREGATION_NONE";
        break;
      case pb::store::SUM:
        ss << "SUM";
        break;
      case pb::store::COUNT:
        ss << "COUNT";
        break;
      case pb::store::COUNTWITHNULL:
        ss << "COUNTWITHNULL";
        break;
      case pb::store::MAX:
        ss << "MAX";
        break;
      case pb::store::MIN:
        ss << "MIN";
        break;
      case pb::store::SUM0:
        ss << "SUM0";
        break;
      case pb::store::AggregationType_INT_MIN_SENTINEL_DO_NOT_USE_:
      case pb::store::AggregationType_INT_MAX_SENTINEL_DO_NOT_USE_:
        break;
    }

    ss << "\tindex_of_column : " << (aggregation_operator.index_of_column());

    COPROCESSOR_LOG << ss.str() << "\n";

    i++;
  }

  COPROCESSOR_LOG
      << "***************************DebugGroupByOperators End*****************************************************";
}

void Utils::DebugInt32Index(const ::google::protobuf::RepeatedField<int32_t>& repeated_field_int32,
                            const std::string& name) {
  COPROCESSOR_LOG
      << "***************************DebugInt32Index Start*****************************************************";
  COPROCESSOR_LOG << name;

  size_t i = 0;

  for (const auto& repeated_field : repeated_field_int32) {
    std::stringstream ss;
    ss << "[" << i << "]";

    ss << "\tvalue : " << repeated_field;

    COPROCESSOR_LOG << ss.str() << "\n";

    i++;
  }

  COPROCESSOR_LOG
      << "***************************DebugInt32Index End*****************************************************";
}

void Utils::DebugCoprocessor(const pb::store::Coprocessor& coprocessor) {
  COPROCESSOR_LOG
      << "***************************DebugCoprocessor Start*****************************************************";

  COPROCESSOR_LOG << "schema_version : " << coprocessor.schema_version();

  Utils::DebugPbSchema(coprocessor.original_schema().schema(), "original_schema");
  COPROCESSOR_LOG << "original_schema : common_id : " << coprocessor.original_schema().common_id();

  Utils::DebugPbSchema(coprocessor.result_schema().schema(), "result_schema");
  COPROCESSOR_LOG << "result_schema : common_id : " << coprocessor.result_schema().common_id();

  Utils::DebugInt32Index(coprocessor.selection_columns(), "selection_columns");

  COPROCESSOR_LOG << "expression : " << fmt::format("{}", Helper::StringToHex(coprocessor.expression()));

  Utils::DebugInt32Index(coprocessor.group_by_columns(), "group_by_columns");

  Utils::DebugGroupByOperators(coprocessor.aggregation_operators(), "aggregation_operators");

  COPROCESSOR_LOG
      << "***************************DebugCoprocessor End*****************************************************";
}

void Utils::DebugColumn(const std::any& column, BaseSchema::Type type, const std::string& name) {
  switch (type) {
    case BaseSchema::Type::kBool: {
      try {
        const std::optional<bool>& value = std::any_cast<std::optional<bool>>(column);
        if (value.has_value()) {
          COPROCESSOR_LOG << name << " std::optional<bool> : " << (value.value() ? "true" : "false");
        } else {
          COPROCESSOR_LOG << name << " std::optional<bool> : null";
        }
      } catch (const std::bad_any_cast& bad) {
        DINGO_LOG(ERROR) << fmt::format("{} {}  any_cast std::optional<bool> failed", name, bad.what());
        return;
      }
      break;
    }
    case BaseSchema::Type::kInteger: {
      try {
        const std::optional<int32_t>& value = std::any_cast<std::optional<int32_t>>(column);
        if (value.has_value()) {
          COPROCESSOR_LOG << name << " std::optional<int32_t> : " << value.value();
        } else {
          COPROCESSOR_LOG << name << " std::optional<int32_t> : null";
        }
      } catch (const std::bad_any_cast& bad) {
        DINGO_LOG(ERROR) << fmt::format("{} {}  any_cast std::optional<int32_t> failed", name, bad.what());
        return;
      }
      break;
    }
    case BaseSchema::Type::kFloat: {
      try {
        const std::optional<float>& value = std::any_cast<std::optional<float>>(column);
        if (value.has_value()) {
          COPROCESSOR_LOG << name << " std::optional<float> : " << value.value();
        } else {
          COPROCESSOR_LOG << name << " std::optional<float> : null";
        }
      } catch (const std::bad_any_cast& bad) {
        DINGO_LOG(ERROR) << fmt::format("{} {}  any_cast std::optional<float> failed", name, bad.what());
        return;
      }
      break;
    }
    case BaseSchema::Type::kLong: {
      try {
        const std::optional<int64_t>& value = std::any_cast<std::optional<int64_t>>(column);
        if (value.has_value()) {
          COPROCESSOR_LOG << name << " std::optional<int64_t> : " << value.value();
        } else {
          COPROCESSOR_LOG << name << " std::optional<int64_t> : null";
        }
      } catch (const std::bad_any_cast& bad) {
        DINGO_LOG(ERROR) << fmt::format("{} {}  any_cast std::optional<int64_t> failed", name, bad.what());
        return;
      }
      break;
    }
    case BaseSchema::Type::kDouble: {
      try {
        const std::optional<double>& value = std::any_cast<std::optional<double>>(column);
        if (value.has_value()) {
          COPROCESSOR_LOG << name << " std::optional<double> : " << value.value();
        } else {
          COPROCESSOR_LOG << name << " std::optional<double> : null";
        }
      } catch (const std::bad_any_cast& bad) {
        DINGO_LOG(ERROR) << fmt::format("{} {}  any_cast std::optional<double> failed", name, bad.what());
        return;
      }
      break;
    }
    case BaseSchema::Type::kString: {
      try {
        const std::optional<std::shared_ptr<std::string>>& value =
            std::any_cast<std::optional<std::shared_ptr<std::string>>>(column);
        if (value.has_value()) {
          COPROCESSOR_LOG << name
                          << " std::optional<std::shared_ptr<std::string>> : " << Helper::StringToHex(*(value.value()));
        } else {
          COPROCESSOR_LOG << name << " std::optional<std::shared_ptr<std::string>> : null";
        }
      } catch (const std::bad_any_cast& bad) {
        DINGO_LOG(ERROR) << fmt::format("{} {}  any_cast std::optional<std::shared_ptr<std::string>> failed", name,
                                        bad.what());
        return;
      }
      break;
    }
    case BaseSchema::Type::kBoolList: {
      try {
        const std::optional<std::shared_ptr<std::vector<bool>>>& value =
            std::any_cast<std::optional<std::shared_ptr<std::vector<bool>>>>(column);
        if (value.has_value()) {
          const auto vector1 = value.value();
          for (const auto& element : *vector1) {
            COPROCESSOR_LOG << name
                            << " std::optional<std::shared_ptr<std::vector<bool>>> : " << (element ? "true" : "false");
          }
        } else {
          COPROCESSOR_LOG << name << " std::optional<std::shared_ptr<std::vector<bool>>> : null";
        }
      } catch (const std::bad_any_cast& bad) {
        DINGO_LOG(ERROR) << fmt::format("{} {}  any_cast std::optional<std::shared_ptr<std::vector<bool>>> failed",
                                        name, bad.what());
        return;
      }
      break;
    }
    case BaseSchema::Type::kIntegerList: {
      try {
        const std::optional<std::shared_ptr<std::vector<int32_t>>>& value =
            std::any_cast<std::optional<std::shared_ptr<std::vector<int32_t>>>>(column);
        if (value.has_value()) {
          const auto vector1 = value.value();
          for (const auto& element : *vector1) {
            COPROCESSOR_LOG << name << " std::optional<std::shared_ptr<std::vector<int32_t>>> : " << element;
          }
        } else {
          COPROCESSOR_LOG << name << " std::optional<std::shared_ptr<std::vector<int32_t>>> : null";
        }
      } catch (const std::bad_any_cast& bad) {
        DINGO_LOG(ERROR) << fmt::format("{} {}  any_cast std::optional<std::shared_ptr<std::vector<int32_t>>> failed",
                                        name, bad.what());
        return;
      }
      break;
    }
    case BaseSchema::Type::kFloatList: {
      try {
        const std::optional<std::shared_ptr<std::vector<float>>>& value =
            std::any_cast<std::optional<std::shared_ptr<std::vector<float>>>>(column);
        if (value.has_value()) {
          const auto vector1 = value.value();
          for (const auto& element : *vector1) {
            COPROCESSOR_LOG << name << " std::optional<std::shared_ptr<std::vector<float>>> : " << element;
          }
        } else {
          COPROCESSOR_LOG << name << " std::optional<std::shared_ptr<std::vector<float>>> : null";
        }
      } catch (const std::bad_any_cast& bad) {
        DINGO_LOG(ERROR) << fmt::format("{} {}  any_cast std::optional<std::shared_ptr<std::vector<float>>> failed",
                                        name, bad.what());
        return;
      }
      break;
    }
    case BaseSchema::Type::kLongList: {
      try {
        const std::optional<std::shared_ptr<std::vector<int64_t>>>& value =
            std::any_cast<std::optional<std::shared_ptr<std::vector<int64_t>>>>(column);
        if (value.has_value()) {
          const auto vector1 = value.value();
          for (const auto& element : *vector1) {
            COPROCESSOR_LOG << name << " std::optional<std::shared_ptr<std::vector<int64_t>>> : " << element;
          }
        } else {
          COPROCESSOR_LOG << name << " std::optional<std::shared_ptr<std::vector<int64_t>>> : null";
        }
      } catch (const std::bad_any_cast& bad) {
        DINGO_LOG(ERROR) << fmt::format("{} {}  any_cast std::optional<std::shared_ptr<std::vector<int64_t>>> failed",
                                        name, bad.what());
        return;
      }
      break;
    }
    case BaseSchema::Type::kDoubleList: {
      try {
        const std::optional<std::shared_ptr<std::vector<double>>>& value =
            std::any_cast<std::optional<std::shared_ptr<std::vector<double>>>>(column);
        if (value.has_value()) {
          const auto vector1 = value.value();
          for (const auto& element : *vector1) {
            COPROCESSOR_LOG << name << " std::optional<std::shared_ptr<std::vector<double>>> : " << element;
          }
        } else {
          COPROCESSOR_LOG << name << " std::optional<std::shared_ptr<std::vector<double>>> : null";
        }
      } catch (const std::bad_any_cast& bad) {
        DINGO_LOG(ERROR) << fmt::format("{} {}  any_cast std::optional<std::shared_ptr<std::vector<double>>> failed",
                                        name, bad.what());
        return;
      }
      break;
    }
    case BaseSchema::Type::kStringList: {
      try {
        const std::optional<std::shared_ptr<std::vector<std::string>>>& value =
            std::any_cast<std::optional<std::shared_ptr<std::vector<std::string>>>>(column);
        if (value.has_value()) {
          const auto vector1 = value.value();
          for (const auto& element : *vector1) {
            COPROCESSOR_LOG << name << " std::optional<std::shared_ptr<std::vector<string>>> : "
                            << Helper::StringToHex(element);
          }
        } else {
          COPROCESSOR_LOG << name << " std::optional<std::shared_ptr<std::vector<std::string>>> : null";
        }
      } catch (const std::bad_any_cast& bad) {
        DINGO_LOG(ERROR) << fmt::format(
            "{} {}  any_cast std::optional<std::shared_ptr<std::vector<std::string>>> failed", name, bad.what());
        return;
      }
      break;
    }
    default: {
      std::string error_message = fmt::format("{} CloneColumn unsupported type  {}", name, static_cast<int>(type));
      DINGO_LOG(ERROR) << error_message;
      return;
    }
  }
}

void Utils::DebugGroupByKey(const std::string& key, const std::string& name) {
  COPROCESSOR_LOG << name << " : " << Helper::StringToHex(key);
}

///////////////////////////////////////////////CoprocessorV2////////////////////////////////////////////////////////////////////
void Utils::DebugCoprocessorV2(const pb::common::CoprocessorV2& coprocessor) {
  // COPROCESSOR_LOG << fmt::format("pb::common::CoprocessorV2 ; {}", coprocessor.ShortDebugString());

  COPROCESSOR_LOG
      << "***************************DebugCoprocessorV2 Start*****************************************************";

  COPROCESSOR_LOG << "schema_version : " << coprocessor.schema_version();

  Utils::DebugPbSchema(coprocessor.original_schema().schema(), "original_schema");
  COPROCESSOR_LOG << "original_schema : common_id : " << coprocessor.original_schema().common_id();

  Utils::DebugPbSchema(coprocessor.result_schema().schema(), "result_schema");
  COPROCESSOR_LOG << "result_schema : common_id : " << coprocessor.result_schema().common_id();

  Utils::DebugInt32Index(coprocessor.selection_columns(), "selection_columns");

  COPROCESSOR_LOG << "expression : " << fmt::format("{}", Helper::StringToHex(coprocessor.rel_expr()));

  COPROCESSOR_LOG
      << "***************************DebugCoprocessorV2 End*****************************************************";
}

void Utils::DebugPrintAnyArray(const std::vector<std::any>& records, const std::string& name) {
  size_t index = 0;
  COPROCESSOR_LOG << "***************************" << name
                  << " Start*****************************************************";
  for (const auto& record : records) {
    Utils::DebugPrintAny(record, index);
    index++;
  }
  COPROCESSOR_LOG << "***************************" << name
                  << " End*****************************************************";
}

void Utils::DebugPrintAny(const std::any& record, size_t index) {
  auto lambda_get_name_function = [](auto t) {
    return abi::__cxa_demangle(typeid(decltype(t)).name(), nullptr, nullptr, nullptr);
  };

  static std::map<const char*, BaseSchema::Type> any_array_map{
      {lambda_get_name_function(std::optional<bool>()), BaseSchema::Type::kBool},
      {lambda_get_name_function(std::optional<int>()), BaseSchema::Type::kInteger},
      {lambda_get_name_function(std::optional<float>()), BaseSchema::Type::kFloat},
      {lambda_get_name_function(std::optional<int64_t>()), BaseSchema::Type::kLong},
      {lambda_get_name_function(std::optional<double>()), BaseSchema::Type::kDouble},
      {lambda_get_name_function(std::optional<std::shared_ptr<std::string>>()), BaseSchema::Type::kString},

      {lambda_get_name_function(std::optional<std::shared_ptr<std::vector<bool>>>()), BaseSchema::Type::kBoolList},
      {lambda_get_name_function(std::optional<std::shared_ptr<std::vector<int>>>()), BaseSchema::Type::kIntegerList},
      {lambda_get_name_function(std::optional<std::shared_ptr<std::vector<float>>>()), BaseSchema::Type::kFloatList},
      {lambda_get_name_function(std::optional<std::shared_ptr<std::vector<int64_t>>>()), BaseSchema::Type::kLongList},
      {lambda_get_name_function(std::optional<std::shared_ptr<std::vector<double>>>()), BaseSchema::Type::kDoubleList},
      {lambda_get_name_function(std::optional<std::shared_ptr<std::vector<std::string>>>()),
       BaseSchema::Type::kStringList},

  };

  const char* type_name = abi::__cxa_demangle(record.type().name(), nullptr, nullptr, nullptr);

  auto lambda_match_function = [&record, index, type_name](
                                   const std::map<const char*, BaseSchema::Type>& any_array_map,  // NOLINT
                                   const char* name) {
    if (0 == strcmp(type_name, name)) {
      std::string value_string = "no value";
      if (record.has_value()) {
        auto iter = any_array_map.find(name);
        if (iter == any_array_map.end()) {
          return false;
        }

        BaseSchema::Type type = iter->second;
        switch (type) {
          case BaseSchema::kBool: {
            try {
              auto value = std::any_cast<std::optional<bool>>(record);
              if (value.has_value()) {
                if (value.value()) {
                  value_string = "true";
                } else {
                  value_string = "false";
                }
              }
            } catch (const std::bad_any_cast& cast) {
              ;  // NOLINT
            }

            break;
          }
          case BaseSchema::kInteger: {
            try {
              auto value = std::any_cast<std::optional<int>>(record);
              if (value.has_value()) {
                value_string = std::to_string(value.value());
              }
            } catch (const std::bad_any_cast& cast) {
              ;  // NOLINT
            }
            break;
          }
          case BaseSchema::kFloat: {
            try {
              auto value = std::any_cast<std::optional<float>>(record);
              if (value.has_value()) {
                value_string = std::to_string(value.value());
              }
            } catch (const std::bad_any_cast& cast) {
              ;  // NOLINT
            }
            break;
          }

          case BaseSchema::kLong: {
            try {
              auto value = std::any_cast<std::optional<int64_t>>(record);
              if (value.has_value()) {
                value_string = std::to_string(value.value());
              }
            } catch (const std::bad_any_cast& cast) {
              ;  // NOLINT
            }
            break;
          }
          case BaseSchema::kDouble: {
            try {
              auto value = std::any_cast<std::optional<double>>(record);
              if (value.has_value()) {
                value_string = std::to_string(value.value());
              }
            } catch (const std::bad_any_cast& cast) {
              ;  // NOLINT
            }
            break;
          }
          case BaseSchema::kString: {
            try {
              auto value = std::any_cast<std::optional<std::shared_ptr<std::string>>>(record);
              if (value.has_value()) {
                value_string = *(value.value());
              }
            } catch (const std::bad_any_cast& cast) {
              ;  // NOLINT
            }
            break;
          }
          case BaseSchema::kBoolList: {
            try {
              auto value = std::any_cast<std::optional<std::shared_ptr<std::vector<bool>>>>(record);
              if (value.has_value()) {
                const std::shared_ptr<std::vector<bool>>& internal_value = value.value();

                if (internal_value) {
                  value_string = "[";
                  size_t i = 0;
                  for (const auto& iv_item : *internal_value) {
                    if (0 != i) {
                      value_string += ", ";
                    }
                    if (iv_item) {
                      value_string += "true";
                    } else {
                      value_string += "false";
                    }
                    i++;
                  }
                  value_string += "]";
                }
              }
            } catch (const std::bad_any_cast& cast) {
              ;  // NOLINT
            }
            break;
          }

          case BaseSchema::kIntegerList: {
            try {
              auto value = std::any_cast<std::optional<std::shared_ptr<std::vector<int>>>>(record);
              if (value.has_value()) {
                const std::shared_ptr<std::vector<int>>& internal_value = value.value();

                if (internal_value) {
                  value_string = "[";
                  size_t i = 0;
                  for (const auto& iv_item : *internal_value) {
                    if (0 != i) {
                      value_string += ", ";
                    }
                    value_string += std::to_string(iv_item);
                    i++;
                  }
                  value_string += "]";
                }
              }
            } catch (const std::bad_any_cast& cast) {
              ;  // NOLINT
            }
            break;
          }
          case BaseSchema::kFloatList: {
            try {
              auto value = std::any_cast<std::optional<std::shared_ptr<std::vector<float>>>>(record);
              if (value.has_value()) {
                const std::shared_ptr<std::vector<float>>& internal_value = value.value();

                if (internal_value) {
                  value_string = "[";
                  size_t i = 0;
                  for (const auto& iv_item : *internal_value) {
                    if (0 != i) {
                      value_string += ", ";
                    }
                    value_string += std::to_string(iv_item);
                    i++;
                  }
                  value_string += "]";
                }
              }
            } catch (const std::bad_any_cast& cast) {
              ;  // NOLINT
            }
            break;
          }
          case BaseSchema::kLongList: {
            try {
              auto value = std::any_cast<std::optional<std::shared_ptr<std::vector<int64_t>>>>(record);
              if (value.has_value()) {
                const std::shared_ptr<std::vector<int64_t>>& internal_value = value.value();

                if (internal_value) {
                  value_string = "[";
                  size_t i = 0;
                  for (const auto& iv_item : *internal_value) {
                    if (0 != i) {
                      value_string += ", ";
                    }
                    value_string += std::to_string(iv_item);
                    i++;
                  }
                  value_string += "]";
                }
              }
            } catch (const std::bad_any_cast& cast) {
              ;  // NOLINT
            }
            break;
          }
          case BaseSchema::kDoubleList: {
            try {
              auto value = std::any_cast<std::optional<std::shared_ptr<std::vector<double>>>>(record);
              if (value.has_value()) {
                const std::shared_ptr<std::vector<double>>& internal_value = value.value();

                if (internal_value) {
                  value_string = "[";
                  size_t i = 0;
                  for (const auto& iv_item : *internal_value) {
                    if (0 != i) {
                      value_string += ", ";
                    }
                    value_string += std::to_string(iv_item);
                    i++;
                  }
                  value_string += "]";
                }
              }
            } catch (const std::bad_any_cast& cast) {
              ;  // NOLINT
            }
            break;
          }
          case BaseSchema::kStringList: {
            try {
              auto value = std::any_cast<std::optional<std::shared_ptr<std::vector<std::string>>>>(record);
              if (value.has_value()) {
                const std::shared_ptr<std::vector<std::string>>& internal_value = value.value();

                if (internal_value) {
                  value_string = "[";
                  size_t i = 0;
                  for (const auto& iv_item : *internal_value) {
                    if (0 != i) {
                      value_string += ", ";
                    }
                    value_string += iv_item;
                    i++;
                  }
                  value_string += "]";
                }
              }
            } catch (const std::bad_any_cast& cast) {
              ;  // NOLINT
            }
            break;
          }
          default: {
            return false;
          }
        }
      }

      COPROCESSOR_LOG_FOR_LAMBDA << type_name << " "
                                 << "index : " << index << " value  : " << value_string;
      return true;
    }
    return false;
  };

  bool is_match = false;
  for (const auto& [name, _] : any_array_map) {
    if (lambda_match_function(any_array_map, name)) {
      is_match = true;
      break;
    }
  }
  if (!is_match) {
    COPROCESSOR_LOG << type_name << " "
                    << "index : " << index << " value  : not match";
  }
}

#undef COPROCESSOR_LOG
#undef COPROCESSOR_LOG_FOR_LAMBDA

}  // namespace dingodb
