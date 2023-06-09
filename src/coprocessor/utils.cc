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

#include <algorithm>
#include <any>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <utility>
#include <vector>

#include "common/helper.h"
#include "common/logging.h"
#include "fmt/core.h"
#include "proto/error.pb.h"
#include "proto/store.pb.h"
#include "serial/record_decoder.h"
#include "serial/record_encoder.h"

namespace dingodb {

#undef COPROCESSOR_LOG
#define COPROCESSOR_LOG DINGO_LOG(DEBUG)

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

butil::Status Utils::CheckPbSchema(const google::protobuf::RepeatedPtrField<pb::store::Schema>& pb_schemas) {
  if (pb_schemas.empty()) {
    std::string error_message = fmt::format("pb_schema empty. not support");
    DINGO_LOG(ERROR) << error_message;
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, error_message);
  }

  size_t i = 0;
  for (const auto& schema : pb_schemas) {
    const auto& type = schema.type();
    if (type != pb::store::Schema::Type::Schema_Type_BOOL && type != pb::store::Schema::Type::Schema_Type_INTEGER &&
        type != pb::store::Schema::Type::Schema_Type_FLOAT && type != pb::store::Schema::Type::Schema_Type_LONG &&
        type != pb::store::Schema::Type::Schema_Type_DOUBLE && type != pb::store::Schema::Type::Schema_Type_STRING) {
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
  if (serial_schemas && serial_schemas->empty()) {
    std::string error_message = fmt::format("serial_schemas empty. not support");
    DINGO_LOG(ERROR) << error_message;
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, error_message);
  }

  size_t i = 0;
  for (const auto& schema : *serial_schemas) {
    const auto& type = schema->GetType();
    if (type != BaseSchema::Type::kBool && type != BaseSchema::Type::kInteger && type != BaseSchema::Type::kFloat &&
        type != BaseSchema::Type::kLong && type != BaseSchema::Type::kDouble && type != BaseSchema::Type::kString) {
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
  if (selection_columns.empty()) {
    std::string error_message = fmt::format("selection_columns empty. not support");
    DINGO_LOG(ERROR) << error_message;
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, error_message);
  }

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

butil::Status Utils::TransToSerialSchema(const google::protobuf::RepeatedPtrField<pb::store::Schema>& pb_schemas,
                                         std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>>* serial_schemas) {
  (*serial_schemas)->reserve(pb_schemas.size());

  size_t index = 0;
  for (const auto& pb_schema : pb_schemas) {
    auto serial_schema_construct_lambda = [](const auto& pb_schema, auto& serial_schema) {
      serial_schema->SetIsKey(pb_schema.is_key());
      serial_schema->SetAllowNull(pb_schema.is_nullable());
      serial_schema->SetIndex(pb_schema.index());
    };

    switch (pb_schema.type()) {
      case pb::store::Schema::Type::Schema_Type_BOOL: {
        SerialBaseSchemaConstructWrapper<decltype(pb_schema), decltype(serial_schema_construct_lambda), bool>(
            pb_schema, serial_schema_construct_lambda, serial_schemas);
        break;
      }
      case pb::store::Schema::Type::Schema_Type_INTEGER: {
        SerialBaseSchemaConstructWrapper<decltype(pb_schema), decltype(serial_schema_construct_lambda), int32_t>(
            pb_schema, serial_schema_construct_lambda, serial_schemas);

        break;
      }
      case pb::store::Schema::Type::Schema_Type_FLOAT: {
        SerialBaseSchemaConstructWrapper<decltype(pb_schema), decltype(serial_schema_construct_lambda), float>(
            pb_schema, serial_schema_construct_lambda, serial_schemas);

        break;
      }
      case pb::store::Schema::Type::Schema_Type_LONG: {
        SerialBaseSchemaConstructWrapper<decltype(pb_schema), decltype(serial_schema_construct_lambda), int64_t>(
            pb_schema, serial_schema_construct_lambda, serial_schemas);

        break;
      }
      case pb::store::Schema::Type::Schema_Type_DOUBLE: {
        SerialBaseSchemaConstructWrapper<decltype(pb_schema), decltype(serial_schema_construct_lambda), double>(
            pb_schema, serial_schema_construct_lambda, serial_schemas);

        break;
      }
      case pb::store::Schema::Type::Schema_Type_STRING: {
        SerialBaseSchemaConstructWrapper<decltype(pb_schema), decltype(serial_schema_construct_lambda),
                                         std::shared_ptr<std::string>>(pb_schema, serial_schema_construct_lambda,
                                                                       serial_schemas);

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
      const std::shared_ptr<BaseSchema>& original_serial_schema = (*old_serial_schemas)[index];
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
        fmt::format("CompareSerialSchemaStrict failed  serial_schema1 invalid uneqaul serial_schema2 valid ");
    DINGO_LOG(ERROR) << error_message;
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, error_message);
  } else if (serial_schemas1 && !serial_schemas2) {
    std::string error_message =
        fmt::format("CompareSerialSchemaStrict failed  serial_schema1 valid  uneqaul serial_schema2 invalid");
    DINGO_LOG(ERROR) << error_message;
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, error_message);
  }

  if (serial_schemas1->size() != serial_schemas2->size()) {
    std::string error_message =
        fmt::format("CompareSerialSchemaStrict failed  serial_schema1 size: {} uneqaul serial_schema2 size : {}",
                    serial_schemas1->size(), serial_schemas2->size());
    DINGO_LOG(ERROR) << error_message;
    return butil::Status(pb::error::EILLEGAL_PARAMTETERS, error_message);
  }

  for (size_t i = 0; i < serial_schemas1->size() && i < serial_schemas2->size(); i++) {
    const auto& serial_schema1 = (*serial_schemas1)[i];
    const auto& serial_schema2 = (*serial_schemas2)[i];
    if (serial_schema1->GetType() != serial_schema2->GetType()) {
      std::string error_message =
          fmt::format("CompareSerialSchemaStrict failed  serial_schema1 type: {} unequal serial_schema2 type : {}",
                      static_cast<int>(serial_schema1->GetType()), static_cast<int>(serial_schema2->GetType()));
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
          "enable CompareSerialSchemaNonStrict failed aggregation index : {}  serial_schema1 type: {} unequal "
          "serial_schema2 type : {}",
          i, static_cast<int>(serial_schema1->GetType()), static_cast<int>(serial_schema2->GetType()));
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
            "enable CompareSerialSchemaNonStrict failed aggregation index : {}  serial_schema1 type: {} unequal "
            "serial_schema2 type : {}",
            j, static_cast<int>(serial_schema1->GetType()), static_cast<int>(serial_schema2->GetType()));
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

void Utils::DebugPbSchema(const google::protobuf::RepeatedPtrField<pb::store::Schema>& pb_schemas,
                          const std::string& name) {
  COPROCESSOR_LOG
      << "***************************DebugPbSchema Start*****************************************************";
  COPROCESSOR_LOG << name;

  size_t i = 0;
  for (const auto& schema : pb_schemas) {
    const auto& type = schema.type();

    COPROCESSOR_LOG << "[" << i << "]";

    // COPROCESSOR_LOG << "Schema_Type :";

    switch (type) {
      case pb::store::Schema_Type_BOOL:
        COPROCESSOR_LOG << "Schema_Type : Schema_Type_BOOL";
        break;
      case pb::store::Schema_Type_INTEGER:
        COPROCESSOR_LOG << "Schema_Type : Schema_Type_INTEGER";
        break;
      case pb::store::Schema_Type_FLOAT:
        COPROCESSOR_LOG << "Schema_Type : Schema_Type_FLOAT";
        break;
      case pb::store::Schema_Type_LONG:
        COPROCESSOR_LOG << "Schema_Type : Schema_Type_LONG";
        break;
      case pb::store::Schema_Type_DOUBLE:
        COPROCESSOR_LOG << "Schema_Type : Schema_Type_DOUBLE";
        break;
      case pb::store::Schema_Type_STRING:
        COPROCESSOR_LOG << "Schema_Type : Schema_Type_STRING";
        break;
    }

    COPROCESSOR_LOG << "is_key : " << (schema.is_key() ? "true" : "false");
    COPROCESSOR_LOG << "is_nullable : " << (schema.is_nullable() ? "true" : "false");
    COPROCESSOR_LOG << "index : " << (schema.index());

    COPROCESSOR_LOG << "\n";
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
      const auto& type = schema->GetType();

      COPROCESSOR_LOG << "[" << i << "]";

      // COPROCESSOR_LOG << "Schema_Type :";

      switch (type) {
        case BaseSchema::kBool:
          COPROCESSOR_LOG << "Schema_Type : BaseSchema::kBool";
          break;
        case BaseSchema::kInteger:
          COPROCESSOR_LOG << "Schema_Type : BaseSchema::kInteger";
          break;
        case BaseSchema::kFloat:
          COPROCESSOR_LOG << "Schema_Type : BaseSchema::kFloat";
          break;
        case BaseSchema::kLong:
          COPROCESSOR_LOG << "Schema_Type : BaseSchema::kLong";
          break;
        case BaseSchema::kDouble:
          COPROCESSOR_LOG << "Schema_Type : BaseSchema::kDouble";
          break;
        case BaseSchema::kString:
          COPROCESSOR_LOG << "Schema_Type : BaseSchema::kString";
          break;
      }

      COPROCESSOR_LOG << "IsKey : " << (schema->IsKey() ? "true" : "false");
      COPROCESSOR_LOG << "AllowNull : " << (schema->AllowNull() ? "true" : "false");
      COPROCESSOR_LOG << "Index : " << (schema->GetIndex());

      COPROCESSOR_LOG << "\n";
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
    const auto& oper = aggregation_operator.oper();

    COPROCESSOR_LOG << "[" << i << "]";

    // COPROCESSOR_LOG << "Oper_Type :";

    switch (oper) {
      case pb::store::AGGREGATION_NONE:
        COPROCESSOR_LOG << "Oper_Type : AGGREGATION_NONE";
        break;
      case pb::store::SUM:
        COPROCESSOR_LOG << "Oper_Type : SUM";
        break;
      case pb::store::COUNT:
        COPROCESSOR_LOG << "Oper_Type : COUNT";
        break;
      case pb::store::COUNTWITHNULL:
        COPROCESSOR_LOG << "Oper_Type : COUNTWITHNULL";
        break;
      case pb::store::MAX:
        COPROCESSOR_LOG << "Oper_Type : MAX";
        break;
      case pb::store::MIN:
        COPROCESSOR_LOG << "Oper_Type : MIN";
        break;
      case pb::store::SUM0:
        COPROCESSOR_LOG << "Oper_Type : SUM0";
        break;
    }

    COPROCESSOR_LOG << "index_of_column : " << (aggregation_operator.index_of_column());

    COPROCESSOR_LOG << "\n";
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
    COPROCESSOR_LOG << "[" << i << "]";

    COPROCESSOR_LOG << "value : " << repeated_field;

    COPROCESSOR_LOG << "\n";
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

  COPROCESSOR_LOG << fmt::format(Helper::StringToHex(coprocessor.expression()));

  Utils::DebugInt32Index(coprocessor.group_by_columns(), "group_by_columns");

  Utils::DebugGroupByOperators(coprocessor.aggregation_operators(), "aggregation_operators");

  COPROCESSOR_LOG
      << "***************************DebugCoprocessor End*****************************************************";
}

void Utils::PrintColumn(const std::any& column, BaseSchema::Type type, const std::string& name) {
  switch (type) {
    case BaseSchema::Type::kBool: {
      try {
        const std::optional<bool>& value = std::any_cast<std::optional<bool>>(column);
        if (value.has_value()) {
          COPROCESSOR_LOG << name << " std::optional<bool> :" << (value.value() ? "true" : "false");
        } else {
          COPROCESSOR_LOG << name << "std::optional<bool> is null";
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
          COPROCESSOR_LOG << name << " std::optional<int32_t> :" << value.value();
        } else {
          COPROCESSOR_LOG << name << " std::optional<int32_t> is null";
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
          COPROCESSOR_LOG << name << " std::optional<float> :" << value.value();
        } else {
          COPROCESSOR_LOG << name << " std::optional<float> is null";
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
          COPROCESSOR_LOG << name << " std::optional<int64_t> :" << value.value();
        } else {
          COPROCESSOR_LOG << name << " std::optional<int64_t> is null";
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
          COPROCESSOR_LOG << name << " std::optional<double> :" << value.value();
        } else {
          COPROCESSOR_LOG << name << " std::optional<double> is null";
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
          COPROCESSOR_LOG << name << " std::optional<std::shared_ptr<std::string>> is null";
        }
      } catch (const std::bad_any_cast& bad) {
        DINGO_LOG(ERROR) << fmt::format("{} {}  any_cast std::optional<std::shared_ptr<std::string>> failed", name,
                                        bad.what());
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

void Utils::PrintGroupByKey(const std::string& key, const std::string& name) {
  COPROCESSOR_LOG << name << " : " << Helper::StringToHex(key);
}

#undef COPROCESSOR_LOG

}  // namespace dingodb
