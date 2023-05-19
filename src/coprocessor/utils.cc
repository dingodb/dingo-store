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
#include "proto/error.pb.h"
#include "proto/store.pb.h"
#include "serial/record_decoder.h"
#include "serial/record_encoder.h"

namespace dingodb {

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

    if (i != schema.index()) {
      std::string error_message = fmt::format("pb_schema invalid index : {} should be {}", schema.index(), i);
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
        elem.oper() != pb::store::AggregationType::MIN) {
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
          fmt::format("CompareSerialSchemaStrict failed  serial_schema1 type: {} uneqaul serial_schema2 type : {}",
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
        fmt::format("CompareSerialSchemaNonStrict failed  serial_schema1 size: {} uneqaul serial_schema2 size : {}",
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
          "enable CompareSerialSchemaStrict failed  serial_schema1 type: {} uneqaul serial_schema2 type : {}",
          static_cast<int>(serial_schema1->GetType()), static_cast<int>(serial_schema2->GetType()));
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
            "enable CompareSerialSchemaNonStrict failed  serial_schema1 type: {} uneqaul serial_schema2 type : {}",
            static_cast<int>(serial_schema1->GetType()), static_cast<int>(serial_schema2->GetType()));
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

}  // namespace dingodb
