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

#include "glog/logging.h"
#include "sdk/expression/langchain_expr_visitor.h"

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

std::string Var::ToString() const { return "Var(Type:" + TypeToString(type) + ", Name: " + name + ")"; }

std::any Val::Accept(LangchainExprVisitor* visitor, void* target) { return visitor->VisitVal(this, target); }

std::string Val::ToString() const {
  std::ostringstream oss;

  oss << "Val(Type: " << TypeToString(type) << ", Name: " + name;
  switch (type) {
    case kBOOL:
      oss << ", Value: " << std::any_cast<TypeOf<kBOOL>>(value);
      break;
    case kINT64:
      oss << ", Value: " << std::any_cast<TypeOf<kINT64>>(value);
      break;
    case kDOUBLE:
      oss << ", Value: " << std::any_cast<TypeOf<kDOUBLE>>(value);
      break;
    case kSTRING:
      oss << ", Value: " << std::any_cast<TypeOf<kSTRING>>(value);
      break;
    default:
      CHECK(false) << "Unknown type: " << static_cast<int>(type);
  }
  oss << ")";

  return oss.str();
}

}  // namespace expression
}  // namespace sdk
}  // namespace dingodb
