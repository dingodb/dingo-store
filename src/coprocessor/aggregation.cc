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

#include "coprocessor/aggregation.h"

#include <cstdint>
#include <exception>
#include <memory>
#include <optional>
#include <string>
#include <type_traits>

#include "common/logging.h"
#include "fmt/core.h"
#include "proto/store.pb.h"

namespace dingodb {

Aggregation::Aggregation() = default;
Aggregation::~Aggregation() { Close(); }

butil::Status Aggregation::Open(
    size_t start_aggregation_operators_index,
    const std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>>& result_serial_schemas) {
  butil::Status status;

  result_record_ = std::make_shared<std::vector<std::any>>();

  result_record_->reserve((*result_serial_schemas).size() - start_aggregation_operators_index);
  for (size_t i = start_aggregation_operators_index; i < result_serial_schemas->size(); i++) {
    auto type = (*result_serial_schemas)[i]->GetType();
    switch (type) {
      case BaseSchema::Type::kBool: {
        result_record_->emplace_back(std::optional<bool>(std::nullopt));
        break;
      }
      case BaseSchema::Type::kInteger: {
        result_record_->emplace_back(std::optional<int32_t>(std::nullopt));
        break;
      }
      case BaseSchema::Type::kFloat: {
        result_record_->emplace_back(std::optional<float>(std::nullopt));
        break;
      }
      case BaseSchema::Type::kLong: {
        result_record_->emplace_back(std::optional<int64_t>(std::nullopt));
        break;
      }
      case BaseSchema::Type::kDouble: {
        result_record_->emplace_back(std::optional<double>(std::nullopt));
        break;
      }
      case BaseSchema::Type::kString: {
        result_record_->emplace_back(std::optional<std::shared_ptr<std::string>>(std::nullopt));
        break;
      }
      default: {
        std::string error_message = fmt::format("unsupported serial_schema1 type: {}", static_cast<int>(type));
        DINGO_LOG(ERROR) << error_message;
        return butil::Status(pb::error::EILLEGAL_PARAMTETERS, error_message);
      }
    }
  }

  return butil::Status();
}

butil::Status Aggregation::Execute(
    const std::vector<std::function<bool(const std::any&, std::any*)>>& aggregation_functions,
    const std::vector<std::any>& group_by_operator_record) {
  bool ret = false;
  for (size_t i = 0; i < group_by_operator_record.size(); i++) {
    ret = aggregation_functions[i](group_by_operator_record[i], &(*result_record_)[i]);
    if (!ret) {
      std::string error_message = fmt::format("Execute failed index :  {}", i);
      DINGO_LOG(ERROR) << error_message;
      return butil::Status(pb::error::EILLEGAL_PARAMTETERS, error_message);
    }
  }
  return butil::Status();
}

void Aggregation::Close() { result_record_.reset(); }

}  // namespace dingodb
