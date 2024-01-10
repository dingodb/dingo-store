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
#include <memory>
#include <optional>
#include <string>
#include <variant>
#include <vector>

#include "butil/status.h"
#include "../libexpr/src/expr/operand.h"

namespace dingodb {

#ifndef DINGO_LIBEXPR_MOCK
#define DINGO_LIBEXPR_MOCK
#endif
#undef DINGO_LIBEXPR_MOCK

#if defined(DINGO_LIBEXPR_MOCK)
namespace expr {
class Operand {
 public:
  template <typename T>
  Operand(T v) : m_data_(v) {}

  Operand([[maybe_unused]] std::nullptr_t v) {}

  Operand() = default;

  bool operator!=(const Operand& v) const { return true; }  // NOLINT

  bool operator!=([[maybe_unused]] std::nullptr_t v) const { return true; }

  template <typename T>
  inline T GetValue() const {
    return std::get<T>(m_data_);
  }

 private:
  std::variant<std::monostate, int32_t, int64_t, bool, float, double, std::shared_ptr<std::string>> m_data_;
};

using Tuple = std::vector<Operand>;

namespace any_optional_data_adaptor {
template <typename T>
Operand ToOperand(const std::any& v) {
  const std::optional<T> opt = std::any_cast<const std::optional<T>>(v);
  if (opt.has_value()) {
    return opt.value();
  }
  return nullptr;
}

template <typename T>
std::any FromOperand(const Operand& v) {
  auto opt = (v != nullptr ? std::optional<T>(v.GetValue<T>()) : std::optional<T>());
  return std::make_any<std::optional<T>>(opt);
}
}  // namespace any_optional_data_adaptor

using Byte = unsigned char;

class RelRunner {
 public:
  RelRunner() = default;
  virtual ~RelRunner() {}  // NOLINT

  const expr::Byte* Decode(const expr::Byte* /*code*/, size_t /*len*/) { return nullptr; }  // NOLINT

  const expr::Tuple* Put(const expr::Tuple* /*tuple*/) const { return nullptr; }  // NOLINT

  const expr::Tuple* Get() const { return nullptr; }  // NOLINT
};

}  // namespace expr

#endif  // DINGO_LIBEXPR_MOCK

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
      const std::vector<int>& result_column_indexes, std::vector<std::any> result_record);
};

}  // namespace dingodb

#endif  // DINGODB_COPROCESSOR_REL_EXPR_HELPER_H_  // NOLINT
