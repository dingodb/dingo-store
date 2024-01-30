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

#include "coprocessor/coprocessor_scalar.h"

#include <any>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "common/logging.h"
#include "fmt/core.h"
#include "proto/error.pb.h"
#include "proto/meta.pb.h"
#include "proto/store.pb.h"
#include "serial/schema/base_schema.h"

namespace dingodb {

bvar::Adder<uint64_t> CoprocessorScalar::bvar_coprocessor_v2_filter_scalar_running_num(
    "dingo_coprocessor_v2_filter_scalar_running_num");
bvar::Adder<uint64_t> CoprocessorScalar::bvar_coprocessor_v2_filter_scalar_total_num(
    "dingo_coprocessor_v2_filter_scalar_total_num");
bvar::LatencyRecorder CoprocessorScalar::coprocessor_v2_filter_scalar_latency(
    "dingo_coprocessor_v2_filter_scalar_latency");

CoprocessorScalar::CoprocessorScalar() = default;
CoprocessorScalar::~CoprocessorScalar() { Close(); }

butil::Status CoprocessorScalar::Open(const std::any& coprocessor) { return CoprocessorV2::Open(coprocessor); }

butil::Status CoprocessorScalar::Execute(IteratorPtr iter, bool key_only, size_t max_fetch_cnt, int64_t max_bytes_rpc,
                                         std::vector<pb::common::KeyValue>* kvs, bool& has_more) {
  return CoprocessorV2::RawCoprocessor::Execute(iter, key_only, max_fetch_cnt, max_bytes_rpc, kvs, has_more);  // NOLINT
}

butil::Status CoprocessorScalar::Execute(TxnIteratorPtr iter, int64_t limit, bool key_only, bool is_reverse,
                                         pb::store::TxnResultInfo& txn_result_info,
                                         std::vector<pb::common::KeyValue>& kvs, bool& has_more, std::string& end_key) {
  return CoprocessorV2::RawCoprocessor::Execute(iter, limit, key_only, is_reverse, txn_result_info, kvs,  // NOLINT
                                                has_more, end_key);
}

butil::Status CoprocessorScalar::Filter(const std::string& key, const std::string& value, bool& is_reserved) {
  return CoprocessorV2::RawCoprocessor::Filter(key, value, is_reserved);  // NOLINT
}

butil::Status CoprocessorScalar::Filter(const pb::common::VectorScalardata& scalar_data, bool& is_reserved) {
  BvarLatencyGuard bvar_guard(&coprocessor_v2_filter_scalar_latency);
  CoprocessorScalar::bvar_coprocessor_v2_filter_scalar_running_num << 1;
  CoprocessorScalar::bvar_coprocessor_v2_filter_scalar_total_num << 1;
  ON_SCOPE_EXIT([&]() { CoprocessorScalar::bvar_coprocessor_v2_filter_scalar_running_num << -1; });
  butil::Status status;

  std::vector<std::any> original_record;
  original_record.reserve(selection_column_indexes_.size());

  status = TransToAnyRecord(scalar_data, original_record);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << status.error_cstr();
    return status;
  }

  std::unique_ptr<std::vector<expr::Operand>> result_operand_ptr;

  status = DoRelExprCore(original_record, result_operand_ptr);
  if (!status.ok()) {
    DINGO_LOG(ERROR) << status.error_cstr();
    return status;
  }

  if (!result_operand_ptr) {
    is_reserved = false;
    return butil::Status();
  }

  is_reserved = true;

  return butil::Status();
}

void CoprocessorScalar::Close() { return CoprocessorV2::Close(); }

butil::Status CoprocessorScalar::TransToAnyRecord(const pb::common::VectorScalardata& scalar_data,
                                                  std::vector<std::any>& original_record) {
  for (int selection_column_index : selection_column_indexes_) {
    auto original_serial_schema = (*original_serial_schemas_)[selection_column_index];
    BaseSchema::Type type = original_serial_schema->GetType();
    const std::string& name = original_serial_schema->GetName();

    auto iter = scalar_data.scalar_data().find(name);
    if (iter == scalar_data.scalar_data().end()) {
      std::string error_message = fmt::format("in scalar_data not find name : {}", name);
      DINGO_LOG(ERROR) << error_message;
      return butil::Status(pb::error::EILLEGAL_PARAMTETERS, error_message);
    }

    const auto& scalar_value = iter->second;
    pb::common::ScalarFieldType field_type = scalar_value.field_type();

    auto lambda_check_misc_function = [&name, &type, &field_type, &scalar_value](BaseSchema::Type base_schema_type) {
      if (base_schema_type != type) {
        std::string error_message =
            fmt::format("field name : {} type not match. schema type : {} field_type : {}", name,
                        BaseSchema::GetTypeString(type), pb::common::ScalarFieldType_Name(field_type));
        LOG(ERROR) << "[" << __PRETTY_FUNCTION__ << "] " << error_message;
        return butil::Status(pb::error::EILLEGAL_PARAMTETERS, error_message);
      }

      if (scalar_value.fields().empty()) {
        std::string error_message = fmt::format("name : {} field_type : {} scalar_value.fields() empty", name,
                                                pb::common::ScalarFieldType_Name(field_type));
        LOG(ERROR) << "[" << __PRETTY_FUNCTION__ << "] " << error_message;
        return butil::Status(pb::error::EILLEGAL_PARAMTETERS, error_message);
      }

      return butil::Status();
    };

    switch (field_type) {
      case pb::common::BOOL: {
        auto status = lambda_check_misc_function(BaseSchema::Type::kBool);
        if (!status.ok()) {
          DINGO_LOG(ERROR) << status.error_cstr();
          return status;
        }
        original_record.emplace_back(std::optional<bool>{scalar_value.fields(0).bool_data()});
        break;
      }

      case pb::common::INT32: {
        auto status = lambda_check_misc_function(BaseSchema::Type::kInteger);
        if (!status.ok()) {
          DINGO_LOG(ERROR) << status.error_cstr();
          return status;
        }

        original_record.emplace_back(std::optional<int32_t>{scalar_value.fields(0).int_data()});
        break;
      }
      case pb::common::INT64: {
        auto status = lambda_check_misc_function(BaseSchema::Type::kLong);
        if (!status.ok()) {
          DINGO_LOG(ERROR) << status.error_cstr();
          return status;
        }

        original_record.emplace_back(std::optional<int64_t>{scalar_value.fields(0).long_data()});
        break;
      }
      case pb::common::FLOAT32: {
        auto status = lambda_check_misc_function(BaseSchema::Type::kFloat);
        if (!status.ok()) {
          DINGO_LOG(ERROR) << status.error_cstr();
          return status;
        }

        original_record.emplace_back(std::optional<float>{scalar_value.fields(0).float_data()});
        break;
      }
      case pb::common::DOUBLE: {
        auto status = lambda_check_misc_function(BaseSchema::Type::kDouble);
        if (!status.ok()) {
          DINGO_LOG(ERROR) << status.error_cstr();
          return status;
        }

        original_record.emplace_back(std::optional<double>{scalar_value.fields(0).double_data()});
        break;
      }
      case pb::common::STRING: {
        auto status = lambda_check_misc_function(BaseSchema::Type::kString);
        if (!status.ok()) {
          DINGO_LOG(ERROR) << status.error_cstr();
          return status;
        }

        original_record.emplace_back(std::optional<std::shared_ptr<std::string>>{
            std::make_shared<std::string>(scalar_value.fields(0).string_data())});
        break;
      }

      case pb::common::INT8:
        [[fallthrough]];
      case pb::common::INT16:
        [[fallthrough]];
      case pb::common::BYTES:
        [[fallthrough]];
      case pb::common::NONE:
        [[fallthrough]];
      case pb::common::ScalarFieldType_INT_MIN_SENTINEL_DO_NOT_USE_:
        [[fallthrough]];
      case pb::common::ScalarFieldType_INT_MAX_SENTINEL_DO_NOT_USE_:
        [[fallthrough]];
      default: {
        std::string error_message =
            fmt::format("field name : {}  not support . schema type : {} field_type : {}", name,
                        BaseSchema::GetTypeString(type), pb::common::ScalarFieldType_Name(field_type));
        DINGO_LOG(ERROR) << error_message;
        return butil::Status(pb::error::EILLEGAL_PARAMTETERS, error_message);
      }
    }
  }

  return butil::Status();
}

}  // namespace dingodb
