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

#include "coprocessor/aggregation_manager.h"

#include <cstdint>
#include <exception>
#include <memory>
#include <optional>
#include <string>
#include <type_traits>
#include <utility>

#include "common/logging.h"
#include "fmt/core.h"
#include "proto/store.pb.h"

namespace dingodb {

template <typename PARAM, typename RESULT>
struct SUM {
  bool operator()(const std::any& param, std::any* result) {
    static_assert(
        !(std::is_same_v<std::string, PARAM> || std::is_same_v<std::string, RESULT> ||
          std::is_same_v<std::shared_ptr<std::string>, PARAM> || std::is_same_v<std::shared_ptr<std::string>, RESULT>),
        "SUM : unsupported shared_ptr<std::string> or std::string");

    try {
      const std::optional<PARAM>& param_value = std::any_cast<const std::optional<PARAM>&>(param);
      std::optional<RESULT>& result_value = std::any_cast<std::optional<RESULT>&>(*result);

      if (param_value.has_value() && !result_value.has_value()) {
        result_value = param_value;
      } else if (!param_value.has_value() && !result_value.has_value()) {
        return true;
      } else if (!param_value.has_value() && result_value.has_value()) {
        return true;
      } else {
        result_value.value() += param_value.value();
      }
    } catch (const std::exception& my_exception) {
      DINGO_LOG(ERROR) << fmt::format("SUM<{},{}> exception : {}", typeid(PARAM).name(), typeid(RESULT).name(),
                                      my_exception.what());
      return false;
    }

    return true;
  }
};

template <typename PARAM, typename RESULT>
struct COUNT {
  bool operator()(const std::any& param, std::any* result) {
    try {
      const std::optional<PARAM>& param_value = std::any_cast<const std::optional<PARAM>&>(param);
      std::optional<RESULT>& result_value = std::any_cast<std::optional<RESULT>&>(*result);

      if (param_value.has_value() && !result_value.has_value()) {
        result_value = 1;
      } else if (!param_value.has_value() && !result_value.has_value()) {
        return true;
      } else if (!param_value.has_value() && result_value.has_value()) {
        return true;
      } else {
        result_value.value() += 1;
      }
    } catch (const std::exception& my_exception) {
      DINGO_LOG(ERROR) << fmt::format("COUNT<{},{}> exception : {}", typeid(PARAM).name(), typeid(RESULT).name(),
                                      my_exception.what());
      return false;
    }

    return true;
  }
};

template <typename PARAM, typename RESULT>
struct COUNTWITHNULL {
  bool operator()([[maybe_unused]] const std::any& param, std::any* result) {
    try {
      std::optional<RESULT>& result_value = std::any_cast<std::optional<RESULT>&>(*result);

      if (!result_value.has_value()) {
        result_value = 1;
      } else {
        result_value.value() += 1;
      }
    } catch (const std::exception& my_exception) {
      DINGO_LOG(ERROR) << fmt::format("COUNTWITHNULL<{},{}> exception : {}", typeid(PARAM).name(),
                                      typeid(RESULT).name(), my_exception.what());
      return false;
    }

    return true;
  }
};

template <typename PARAM, typename RESULT>
struct MAX {
  bool operator()(const std::any& param, std::any* result) {
    try {
      const std::optional<PARAM>& param_value = std::any_cast<const std::optional<PARAM>&>(param);
      std::optional<RESULT>& result_value = std::any_cast<std::optional<RESULT>&>(*result);

      if (param_value.has_value() && !result_value.has_value()) {
        if constexpr (std::is_same_v<std::shared_ptr<std::string>, PARAM> &&
                      !std::is_same_v<std::shared_ptr<std::string>, RESULT>) {
          result_value = *(param_value.value());

        } else if constexpr (!std::is_same_v<std::shared_ptr<std::string>, PARAM> &&  // NOLINT
                             std::is_same_v<std::shared_ptr<std::string>, RESULT>) {
          result_value.value() = std::make_shared<std::string>(param_value);

        } else {
          result_value = param_value;
        }

      } else if (!param_value.has_value() && !result_value.has_value()) {
        return true;
      } else if (!param_value.has_value() && result_value.has_value()) {
        return true;
      } else {
        if constexpr (std::is_same_v<std::shared_ptr<std::string>, PARAM> &&
                      !std::is_same_v<std::shared_ptr<std::string>, RESULT>) {
          if (result_value.value() < *(param_value.value())) {
            result_value = *(param_value.value());
          }
        } else if constexpr (std::is_same_v<std::shared_ptr<std::string>, PARAM> &&  // NOLINT
                             std::is_same_v<std::shared_ptr<std::string>, RESULT>) {
          if (*(result_value.value()) < *(param_value.value())) {
            result_value = param_value;
          }
        } else if constexpr (!std::is_same_v<std::shared_ptr<std::string>, PARAM> &&  // NOLINT
                             std::is_same_v<std::shared_ptr<std::string>, RESULT>) {
          if (*(result_value.value()) < param_value.value()) {
            *(result_value.value()) = param_value;
          }
        } else {
          if (result_value.value() < param_value.value()) {
            result_value = param_value;
          }
        }
      }
    } catch (const std::exception& my_exception) {
      DINGO_LOG(ERROR) << fmt::format("MAX<{},{}> exception : {}", typeid(PARAM).name(), typeid(RESULT).name(),
                                      my_exception.what());
      return false;
    }

    return true;
  }
};

template <typename PARAM, typename RESULT>
struct MIN {
  bool operator()(const std::any& param, std::any* result) {
    try {
      const std::optional<PARAM>& param_value = std::any_cast<const std::optional<PARAM>&>(param);
      std::optional<RESULT>& result_value = std::any_cast<std::optional<RESULT>&>(*result);

      if (param_value.has_value() && !result_value.has_value()) {
        if constexpr (std::is_same_v<std::shared_ptr<std::string>, PARAM> &&
                      !std::is_same_v<std::shared_ptr<std::string>, RESULT>) {
          result_value = *(param_value.value());

        } else if constexpr (!std::is_same_v<std::shared_ptr<std::string>, PARAM> &&  // NOLINT
                             std::is_same_v<std::shared_ptr<std::string>, RESULT>) {
          result_value.value() = std::make_shared<std::string>(param_value);

        } else {
          result_value = param_value;
        }

      } else if (!param_value.has_value() && !result_value.has_value()) {
        return true;
      } else if (!param_value.has_value() && result_value.has_value()) {
        return true;
      } else {
        if constexpr (std::is_same_v<std::shared_ptr<std::string>, PARAM> &&
                      !std::is_same_v<std::shared_ptr<std::string>, RESULT>) {
          if (result_value.value() > *(param_value.value())) {
            result_value = *(param_value.value());
          }
        } else if constexpr (std::is_same_v<std::shared_ptr<std::string>, PARAM> &&  // NOLINT
                             std::is_same_v<std::shared_ptr<std::string>, RESULT>) {
          if (*(result_value.value()) > *(param_value.value())) {
            result_value = param_value;
          }
        } else if constexpr (!std::is_same_v<std::shared_ptr<std::string>, PARAM> &&  // NOLINT
                             std::is_same_v<std::shared_ptr<std::string>, RESULT>) {
          if (*(result_value.value()) > param_value.value()) {
            *(result_value.value()) = param_value;
          }
        } else {
          if (result_value.value() > param_value.value()) {
            result_value = param_value;
          }
        }
      }
    } catch (const std::exception& my_exception) {
      DINGO_LOG(ERROR) << fmt::format("MIN<{},{}> exception : {}", typeid(PARAM).name(), typeid(RESULT).name(),
                                      my_exception.what());
      return false;
    }

    return true;
  }
};

AggregationManager::AggregationManager() = default;
AggregationManager::~AggregationManager() { Close(); }

butil::Status AggregationManager::Open(
    const std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>>& group_by_operator_serial_schemas,
    const ::google::protobuf::RepeatedPtrField<pb::store::AggregationOperator>& aggregation_operators,
    const std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>>& result_serial_schemas) {
  butil::Status status;
  group_by_operator_serial_schemas_ = group_by_operator_serial_schemas;
  aggregation_operators_ = aggregation_operators;
  result_serial_schemas_ = result_serial_schemas;

  size_t start_aggregation_operators_index = result_serial_schemas->size() - aggregation_operators.size();

  size_t i = 0;
  aggregation_functions_.reserve(aggregation_operators.size());
  for (const auto& aggregation_operator : aggregation_operators) {
    int32_t index = aggregation_operator.index_of_column();
    const auto& oper = aggregation_operator.oper();
    BaseSchema::Type serial_schema_type = (*group_by_operator_serial_schemas)[i]->GetType();
    BaseSchema::Type result_schema_type = (*result_serial_schemas)[i + start_aggregation_operators_index]->GetType();
    switch (oper) {
      case pb::store::AggregationType::SUM0:
        [[fallthrough]];
      case pb::store::AggregationType::SUM: {
        status = AddSumFunction(serial_schema_type, result_schema_type);
        if (!status.ok()) {
          DINGO_LOG(ERROR) << fmt::format(
              "AddSumFunction failed index : {} serial_schema_type : {} result_schema_type : {}", index,
              BaseSchema::GetTypeString(serial_schema_type), BaseSchema::GetTypeString(result_schema_type));
          return status;
        }
        break;
      }
      case pb::store::AggregationType::COUNT: {
        if (-1 == index) {
          status = AddCountWithNullFunction(serial_schema_type, result_schema_type);
          if (!status.ok()) {
            DINGO_LOG(ERROR) << fmt::format(
                "AddCountWithNullFunction failed index : {} serial_schema_type : {} result_schema_type : {}", index,
                BaseSchema::GetTypeString(serial_schema_type), BaseSchema::GetTypeString(result_schema_type));
            return status;
          }
          break;
        }
        status = AddCountFunction(serial_schema_type, result_schema_type);
        if (!status.ok()) {
          DINGO_LOG(ERROR) << fmt::format(
              "AddCountFunction failed index : {} serial_schema_type : {} result_schema_type : {}", index,
              BaseSchema::GetTypeString(serial_schema_type), BaseSchema::GetTypeString(result_schema_type));
          return status;
        }

        break;
      }
      case pb::store::AggregationType::COUNTWITHNULL: {
        status = AddCountWithNullFunction(serial_schema_type, result_schema_type);
        if (!status.ok()) {
          DINGO_LOG(ERROR) << fmt::format(
              "AddCountWithNullFunction failed index : {} serial_schema_type : {} result_schema_type : {}", index,
              BaseSchema::GetTypeString(serial_schema_type), BaseSchema::GetTypeString(result_schema_type));
          return status;
        }
        break;
      }
      case pb::store::AggregationType::MAX: {
        status = AddMaxFunction(serial_schema_type, result_schema_type);
        if (!status.ok()) {
          DINGO_LOG(ERROR) << fmt::format(
              "AddMaxFunction failed index : {} serial_schema_type : {} result_schema_type : {}", index,
              BaseSchema::GetTypeString(serial_schema_type), BaseSchema::GetTypeString(result_schema_type));
          return status;
        }
        break;
      }
      case pb::store::AggregationType::MIN: {
        status = AddMinFunction(serial_schema_type, result_schema_type);
        if (!status.ok()) {
          DINGO_LOG(ERROR) << fmt::format(
              "AddMinFunction failed index : {} serial_schema_type : {} result_schema_type : {}", index,
              BaseSchema::GetTypeString(serial_schema_type), BaseSchema::GetTypeString(result_schema_type));
          return status;
        }
        break;
      }
      case pb::store::AggregationType::AGGREGATION_NONE:
        [[fallthrough]];
      default: {
        std::string error_message = fmt::format("unsupported pb_schema1 oper: {}", static_cast<int>(oper));
        DINGO_LOG(ERROR) << error_message;
        return butil::Status(pb::error::ENOT_SUPPORT, error_message);
      }
    }
    i++;
  }

  return butil::Status();
}

butil::Status AggregationManager::Execute(const std::string& group_by_key,
                                          const std::vector<std::any>& group_by_operator_record) {
  butil::Status status;
  std::shared_ptr<Aggregation> aggregation;

  if (!aggregations_) {
    using MapType = std::map<std::string, std::shared_ptr<Aggregation>>;
    aggregations_ = std::make_shared<MapType>();
  }

  const auto& iter = aggregations_->find(group_by_key);
  if (iter == aggregations_->end()) {
    const auto& [iter_new, _] = aggregations_->emplace(group_by_key, std::make_shared<Aggregation>());
    aggregation = iter_new->second;

    status = aggregation->Open(result_serial_schemas_->size() - group_by_operator_serial_schemas_->size(),
                               result_serial_schemas_, aggregation_operators_);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << fmt::format("Aggregation::Open failed");
      return status;
    }
  } else {
    aggregation = iter->second;
  }

  status = aggregation->Execute(aggregation_functions_, group_by_operator_record);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << fmt::format("Aggregation::Execute failed");
    return status;
  }

  return butil::Status();
}

void AggregationManager::Close() {
  if (group_by_operator_serial_schemas_) {
    group_by_operator_serial_schemas_.reset();
  }

  aggregation_operators_.Clear();

  if (result_serial_schemas_) {
    result_serial_schemas_.reset();
  }

  aggregation_functions_.clear();

  if (aggregations_) {
    aggregations_.reset();
  }
}

std::shared_ptr<AggregationIterator> AggregationManager::CreateIterator() {
  if (!aggregations_) {
    using MapType = std::map<std::string, std::shared_ptr<Aggregation>>;
    aggregations_ = std::make_shared<MapType>();
  }
  DINGO_LOG(DEBUG) << "aggregations  size : " << aggregations_->size();
  return std::make_shared<AggregationIterator>(aggregations_);
}

butil::Status AggregationManager::AddSumFunction(BaseSchema::Type serial_schema_type,
                                                 BaseSchema::Type result_schema_type) {
  if (serial_schema_type == BaseSchema::kBool && result_schema_type == BaseSchema::kBool) {
    aggregation_functions_.emplace_back(SUM<bool, bool>());
  } else if (serial_schema_type == BaseSchema::kInteger && result_schema_type == BaseSchema::kInteger) {
    aggregation_functions_.emplace_back(SUM<int32_t, int32_t>());
  } else if (serial_schema_type == BaseSchema::kFloat && result_schema_type == BaseSchema::kFloat) {
    aggregation_functions_.emplace_back(SUM<float, float>());
  } else if (serial_schema_type == BaseSchema::kLong && result_schema_type == BaseSchema::kLong) {
    aggregation_functions_.emplace_back(SUM<int64_t, int64_t>());
  } else if (serial_schema_type == BaseSchema::kDouble && result_schema_type == BaseSchema::kDouble) {
    aggregation_functions_.emplace_back(SUM<double, double>());
  } else {
    std::string error_message =
        fmt::format("SUM<{},{}>  not support yet", BaseSchema::GetTypeString(serial_schema_type),
                    BaseSchema::GetTypeString(result_schema_type));
    DINGO_LOG(ERROR) << error_message;
    return butil::Status(pb::error::ENOT_SUPPORT, error_message);
  }

  return butil::Status();
}

butil::Status AggregationManager::AddCountFunction(BaseSchema::Type serial_schema_type,
                                                   BaseSchema::Type result_schema_type) {
  if (serial_schema_type == BaseSchema::kBool && result_schema_type == BaseSchema::kLong) {
    aggregation_functions_.emplace_back(COUNT<bool, int64_t>());
  } else if (serial_schema_type == BaseSchema::kInteger && result_schema_type == BaseSchema::kLong) {
    aggregation_functions_.emplace_back(COUNT<int32_t, int64_t>());
  } else if (serial_schema_type == BaseSchema::kFloat && result_schema_type == BaseSchema::kLong) {
    aggregation_functions_.emplace_back(COUNT<float, int64_t>());
  } else if (serial_schema_type == BaseSchema::kLong && result_schema_type == BaseSchema::kLong) {
    aggregation_functions_.emplace_back(COUNT<int64_t, int64_t>());
  } else if (serial_schema_type == BaseSchema::kDouble && result_schema_type == BaseSchema::kLong) {
    aggregation_functions_.emplace_back(COUNT<double, int64_t>());
  } else if (serial_schema_type == BaseSchema::kString && result_schema_type == BaseSchema::kLong) {
    aggregation_functions_.emplace_back(COUNT<std::shared_ptr<std::string>, int64_t>());
  } else {
    std::string error_message =
        fmt::format("COUNT<{},{}>  not support yet", BaseSchema::GetTypeString(serial_schema_type),
                    BaseSchema::GetTypeString(result_schema_type));
    DINGO_LOG(ERROR) << error_message;
    return butil::Status(pb::error::ENOT_SUPPORT, error_message);
  }
  return butil::Status();
}
butil::Status AggregationManager::AddCountWithNullFunction(BaseSchema::Type serial_schema_type,
                                                           BaseSchema::Type result_schema_type) {
  if (serial_schema_type == BaseSchema::kBool && result_schema_type == BaseSchema::kLong) {
    aggregation_functions_.emplace_back(COUNTWITHNULL<bool, int64_t>());
  } else if (serial_schema_type == BaseSchema::kInteger && result_schema_type == BaseSchema::kLong) {
    aggregation_functions_.emplace_back(COUNTWITHNULL<int32_t, int64_t>());
  } else if (serial_schema_type == BaseSchema::kFloat && result_schema_type == BaseSchema::kLong) {
    aggregation_functions_.emplace_back(COUNTWITHNULL<float, int64_t>());
  } else if (serial_schema_type == BaseSchema::kLong && result_schema_type == BaseSchema::kLong) {
    aggregation_functions_.emplace_back(COUNTWITHNULL<int64_t, int64_t>());
  } else if (serial_schema_type == BaseSchema::kDouble && result_schema_type == BaseSchema::kLong) {
    aggregation_functions_.emplace_back(COUNTWITHNULL<double, int64_t>());
  } else if (serial_schema_type == BaseSchema::kString && result_schema_type == BaseSchema::kLong) {
    aggregation_functions_.emplace_back(COUNTWITHNULL<std::shared_ptr<std::string>, int64_t>());
  } else {
    std::string error_message =
        fmt::format("COUNTWITHNULL<{},{}>  not support yet", BaseSchema::GetTypeString(serial_schema_type),
                    BaseSchema::GetTypeString(result_schema_type));
    DINGO_LOG(ERROR) << error_message;
    return butil::Status(pb::error::ENOT_SUPPORT, error_message);
  }
  return butil::Status();
}
butil::Status AggregationManager::AddMaxFunction(BaseSchema::Type serial_schema_type,
                                                 BaseSchema::Type result_schema_type) {
  if (serial_schema_type == BaseSchema::kBool && result_schema_type == BaseSchema::kBool) {
    aggregation_functions_.emplace_back(MAX<bool, bool>());
  } else if (serial_schema_type == BaseSchema::kInteger && result_schema_type == BaseSchema::kInteger) {
    aggregation_functions_.emplace_back(MAX<int32_t, int32_t>());
  } else if (serial_schema_type == BaseSchema::kFloat && result_schema_type == BaseSchema::kFloat) {
    aggregation_functions_.emplace_back(MAX<float, float>());
  } else if (serial_schema_type == BaseSchema::kLong && result_schema_type == BaseSchema::kLong) {
    aggregation_functions_.emplace_back(MAX<int64_t, int64_t>());
  } else if (serial_schema_type == BaseSchema::kDouble && result_schema_type == BaseSchema::kDouble) {
    aggregation_functions_.emplace_back(MAX<double, double>());
  } else if (serial_schema_type == BaseSchema::kString && result_schema_type == BaseSchema::kString) {
    aggregation_functions_.emplace_back(MAX<std::shared_ptr<std::string>, std::shared_ptr<std::string>>());
  } else {
    std::string error_message =
        fmt::format("COUNTWITHNULL<{},{}>  not support yet", BaseSchema::GetTypeString(serial_schema_type),
                    BaseSchema::GetTypeString(result_schema_type));
    DINGO_LOG(ERROR) << error_message;
    return butil::Status(pb::error::ENOT_SUPPORT, error_message);
  }
  return butil::Status();
}
butil::Status AggregationManager::AddMinFunction(BaseSchema::Type serial_schema_type,
                                                 BaseSchema::Type result_schema_type) {
  if (serial_schema_type == BaseSchema::kBool && result_schema_type == BaseSchema::kBool) {
    aggregation_functions_.emplace_back(MIN<bool, bool>());
  } else if (serial_schema_type == BaseSchema::kInteger && result_schema_type == BaseSchema::kInteger) {
    aggregation_functions_.emplace_back(MIN<int32_t, int32_t>());
  } else if (serial_schema_type == BaseSchema::kFloat && result_schema_type == BaseSchema::kFloat) {
    aggregation_functions_.emplace_back(MIN<float, float>());
  } else if (serial_schema_type == BaseSchema::kLong && result_schema_type == BaseSchema::kLong) {
    aggregation_functions_.emplace_back(MIN<int64_t, int64_t>());
  } else if (serial_schema_type == BaseSchema::kDouble && result_schema_type == BaseSchema::kDouble) {
    aggregation_functions_.emplace_back(MIN<double, double>());
  } else if (serial_schema_type == BaseSchema::kString && result_schema_type == BaseSchema::kString) {
    aggregation_functions_.emplace_back(MIN<std::shared_ptr<std::string>, std::shared_ptr<std::string>>());
  } else {
    std::string error_message =
        fmt::format("COUNTWITHNULL<{},{}>  not support yet", BaseSchema::GetTypeString(serial_schema_type),
                    BaseSchema::GetTypeString(result_schema_type));
    DINGO_LOG(ERROR) << error_message;
    return butil::Status(pb::error::ENOT_SUPPORT, error_message);
  }
  return butil::Status();
}

}  // namespace dingodb
