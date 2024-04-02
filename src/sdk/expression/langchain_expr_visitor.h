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

#ifndef DINGODB_SDK_EXPRESSION_LANGCHAIN_EXPR_VISITOR_H_
#define DINGODB_SDK_EXPRESSION_LANGCHAIN_EXPR_VISITOR_H_

#include <any>
#include <memory>

#include "sdk/expression/langchain_expr.h"

namespace dingodb {
namespace sdk {
namespace expression {

class LangchainExprVisitor {
 public:
  virtual ~LangchainExprVisitor() = default;

  virtual std::any Visit(LangchainExpr* expr, void* target) { return expr->Accept(this, target); }

  virtual std::any VisitAndOperatorExpr(AndOperatorExpr* expr, void* target) = 0;

  virtual std::any VisitOrOperatorExpr(OrOperatorExpr* expr, void* target) = 0;

  virtual std::any VisitNotOperatorExpr(NotOperatorExpr* expr, void* target) = 0;

  virtual std::any VisitEqComparatorExpr(EqComparatorExpr* expr, void* target) = 0;

  virtual std::any VisitNeComparatorExpr(NeComparatorExpr* expr, void* target) = 0;

  virtual std::any VisitGteComparatorExpr(GteComparatorExpr* expr, void* target) = 0;

  virtual std::any VisitGtComparatorExpr(GtComparatorExpr* expr, void* target) = 0;

  virtual std::any VisitLteComparatorExpr(LteComparatorExpr* expr, void* target) = 0;

  virtual std::any VisitLtComparatorExpr(LtComparatorExpr* expr, void* target) = 0;

  virtual std::any VisitVar(Var* expr, void* target) = 0;

  virtual std::any VisitVal(Val* expr, void* target) = 0;
};

}  // namespace expression
}  // namespace sdk
}  // namespace dingodb
#endif  // DINGODB_SDK_EXPRESSION_LANGCHAIN_EXPR_VISITOR_H_