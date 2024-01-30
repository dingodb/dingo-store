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

#ifndef DINGODB_COPROCESSOR_REL_EXPR_HELPER_H_  // NOLINT
#define DINGODB_COPROCESSOR_REL_EXPR_HELPER_H_

#include <serial/schema/base_schema.h>

#include <any>
#include <deque>  // IWYU pragma: keep
#include <memory>
#include <vector>

#include "../libexpr/src/expr/operand.h"
#include "butil/status.h"

namespace dingodb {

#ifndef TEST_COPROCESSOR_V2_MOCK
#define TEST_COPROCESSOR_V2_MOCK
#endif

#undef TEST_COPROCESSOR_V2_MOCK

#if defined(TEST_COPROCESSOR_V2_MOCK)

namespace rel::mock {

class RelRunner {
 public:
  RelRunner() = default;
  virtual ~RelRunner() = default;

  RelRunner& operator=(const RelRunner&) = default;

  const expr::Byte* Decode(const expr::Byte* /*code*/, size_t /*len*/) { return nullptr; }

  const expr::Tuple* Put(const expr::Tuple* tuple) {
    if (index_ % 2 == 0) {
      index_++;
      return tuple;
    } else {
      tuples_.emplace_back(const_cast<expr::Tuple*>(tuple));
      index_++;
      return nullptr;
    }
  }

  const expr::Tuple* Get() {
    const expr::Tuple* tuple = nullptr;
    if (!tuples_.empty()) {
      tuple = tuples_.back();
      tuples_.pop_back();
    }

    return tuple;
  }

 private:
  std::deque<expr::Tuple*> tuples_;
  size_t index_ = 0;
};

}  // namespace rel::mock

#endif  // #if defined(TEST_COPROCESSOR_V2_MOCK)

class RelExprHelper {
 public:
  RelExprHelper() = delete;
  ~RelExprHelper() = delete;

  RelExprHelper(const RelExprHelper& rhs) = delete;
  RelExprHelper& operator=(const RelExprHelper& rhs) = delete;
  RelExprHelper(RelExprHelper&& rhs) = delete;
  RelExprHelper& operator=(RelExprHelper&& rhs) = delete;

  static butil::Status TransToOperand(BaseSchema::Type type, const std::any& column,
                                      std::unique_ptr<std::vector<expr::Operand>>& operand_ptr);  // NOLINT

  static butil::Status TransFromOperand(BaseSchema::Type type,
                                        const std::unique_ptr<std::vector<expr::Operand>>& operand_ptr, size_t index,
                                        std::vector<std::any>& columns);  // NOLINT

  static butil::Status TransToOperandWrapper(
      const std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>>& original_serial_schemas,
      const std::vector<int>& selection_column_indexes, const std::vector<std::any>& original_record,
      std::unique_ptr<std::vector<expr::Operand>>& operand_ptr);  // NOLINT

  static butil::Status TransFromOperandWrapper(
      const std::unique_ptr<std::vector<expr::Operand>>& operand_ptr,
      const std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>>& result_serial_schemas,
      const std::vector<int>& result_column_indexes, std::vector<std::any>& result_record);
};

}  // namespace dingodb

#endif  // DINGODB_COPROCESSOR_REL_EXPR_HELPER_H_  // NOLINT
