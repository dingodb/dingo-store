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
#include "sdk/port/common.pb.h"

namespace dingodb {
namespace sdk {
namespace expression {

class LangchainExprFactory {
 public:
  LangchainExprFactory() = default;
  virtual ~LangchainExprFactory() = default;

  Status CreateExpr(const std::string& expr_json_str, std::shared_ptr<LangchainExpr>& expr);

  virtual Status MaybeRemapType(const std::string& name, Type& type);
};

class SchemaLangchainExprFactory : public LangchainExprFactory {
 public:
  SchemaLangchainExprFactory(const pb::common::ScalarSchema& schema);

  ~SchemaLangchainExprFactory() override = default;

  Status MaybeRemapType(const std::string& name, Type& type) override;

 private:
  std::unordered_map<std::string, Type> attribute_type_;
};

}  // namespace expression
}  // namespace sdk
}  // namespace dingodb

#endif  // DINGODB_SDK_EXPRESSION_LANGCHAIN_EXPR_FACTORY_H_