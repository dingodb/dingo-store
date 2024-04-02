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

#include "sdk/expression/langchain_expr.h"

#include <cstdint>

#include "sdk/expression/langchain_expr_visitor.h"
#include "sdk/expression/types.h"

namespace dingodb {
namespace sdk {
namespace expression {

std::string OperatorTypeToString(OperatorType operator_type) {
  switch (operator_type) {
    case kAnd:
      return "kAnd";
    case kOr:
      return "kOr";
    case kNot:
      return "kNot";
    default:
      return "Unknown OperatorType";
  }
}

std::string ComparatorTypeToString(ComparatorType comparator_type) {
  switch (comparator_type) {
    case kEq:
      return "kEq";
    case kNe:
      return "kNe";
    case kGte:
      return "kGte";
    case kGt:
      return "kGt";
    case kLte:
      return "kLte";
    case kLt:
      return "kLt";
    default:
      return "Unknown ComparatorType";
  }
}

std::string OperatorExpr::ToString() const {
  std::ostringstream oss;
  oss << "OperatorExpr: " << OperatorTypeToString(operator_type) << "(";
  for (size_t i = 0; i < args.size(); ++i) {
    oss << args[i]->ToString();
    if (i != args.size() - 1) {
      oss << ", ";
    }
  }
  oss << ")";
  return oss.str();
}

std::any AndOperatorExpr::Accept(LangchainExprVisitor* visitor, void* target) {
  return visitor->VisitAndOperatorExpr(this, target);
}

std::any OrOperatorExpr::Accept(LangchainExprVisitor* visitor, void* target) {
  return visitor->VisitOrOperatorExpr(this, target);
}

std::any NotOperatorExpr::Accept(LangchainExprVisitor* visitor, void* target) {
  return visitor->VisitNotOperatorExpr(this, target);
}

std::string ComparatorExpr::ToString() const {
  std::ostringstream oss;
  oss << "ComparatorExpr(ComparatorType: " << ComparatorTypeToString(comparator_type);
  oss << ", Var: " << var->ToString() << ", Val: " << val->ToString() << ")";
  return oss.str();
}

std::any EqComparatorExpr::Accept(LangchainExprVisitor* visitor, void* target) {
  return visitor->VisitEqComparatorExpr(this, target);
}

std::any NeComparatorExpr::Accept(LangchainExprVisitor* visitor, void* target) {
  return visitor->VisitNeComparatorExpr(this, target);
}

std::any GteComparatorExpr::Accept(LangchainExprVisitor* visitor, void* target) {
  return visitor->VisitGteComparatorExpr(this, target);
}

std::any GtComparatorExpr::Accept(LangchainExprVisitor* visitor, void* target) {
  return visitor->VisitGtComparatorExpr(this, target);
}

std::any LteComparatorExpr::Accept(LangchainExprVisitor* visitor, void* target) {
  return visitor->VisitLteComparatorExpr(this, target);
}

std::any LtComparatorExpr::Accept(LangchainExprVisitor* visitor, void* target) {
  return visitor->VisitLtComparatorExpr(this, target);
}

std::any Var::Accept(LangchainExprVisitor* visitor, void* target) { return visitor->VisitVar(this, target); }

std::string Var::ToString() const { return "Var(" + name + ", " + TypeToString(type) + ")"; }

std::any Val::Accept(LangchainExprVisitor* visitor, void* target) { return visitor->VisitVal(this, target); }

std::string Val::ToString() const {
  std::ostringstream oss;

  oss << "Val(Type: " << TypeToString(type);
  switch (type) {
    case STRING:
      oss << ", Value: " << std::any_cast<TypeOf<STRING>>(value);
      break;
    case DOUBLE:
      oss << ", Value: " << std::any_cast<TypeOf<DOUBLE>>(value);
      break;
    case BOOL:
      oss << ", Value: " << std::any_cast<TypeOf<BOOL>>(value);
      break;
    case INT64:
      oss << ", Value: " << std::any_cast<TypeOf<INT64>>(value);
      break;
  }
  oss << ")";

  return oss.str();
}

}  // namespace expression
}  // namespace sdk
}  // namespace dingodb
