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

#ifndef DINGODB_SDK_EXPRESSION_LANGCHAIN_EXPR_FACTORY_H_
#define DINGODB_SDK_EXPRESSION_LANGCHAIN_EXPR_FACTORY_H_

#include <memory>

#include "sdk/expression/langchain_expr.h"
#include "sdk/status.h"

namespace dingodb {
namespace sdk {
namespace expression {

class LangchainExprFactory {
 public:
  LangchainExprFactory() = default;
  ~LangchainExprFactory() = default;

  static Status CreateExpr(const std::string& expr_json_str, std::shared_ptr<LangchainExpr>& expr);
};

}  // namespace expression
}  // namespace sdk
}  // namespace dingodb

#endif  // DINGODB_SDK_EXPRESSION_LANGCHAIN_EXPR_FACTORY_H_