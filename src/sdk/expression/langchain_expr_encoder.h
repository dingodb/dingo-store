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

#ifndef DINGODB_SDK_EXPRESSION_LANGCHAIN_EXPR_ENCODER_H_
#define DINGODB_SDK_EXPRESSION_LANGCHAIN_EXPR_ENCODER_H_

#include <cstdint>
#include <unordered_map>
#include <utility>

#include "proto/common.pb.h"
#include "sdk/expression/langchain_expr.h"
#include "sdk/expression/langchain_expr_visitor.h"
#include "sdk/expression/types.h"

namespace dingodb {
namespace sdk {
namespace expression {

class LangChainExprEncoder : public LangchainExprVisitor {
 public:
  LangChainExprEncoder() = default;
  ~LangChainExprEncoder() override = default;

  pb::common::CoprocessorV2 EncodeToCoprocessor(LangchainExpr* expr);

  std::string EncodeToFilter(LangchainExpr* expr);

  std::any VisitAndOperatorExpr(AndOperatorExpr* expr, void* target) override;

  std::any VisitOrOperatorExpr(OrOperatorExpr* expr, void* target) override;

  std::any VisitNotOperatorExpr(NotOperatorExpr* expr, void* target) override;

  std::any VisitEqComparatorExpr(EqComparatorExpr* expr, void* target) override;

  std::any VisitNeComparatorExpr(NeComparatorExpr* expr, void* target) override;

  std::any VisitGteComparatorExpr(GteComparatorExpr* expr, void* target) override;

  std::any VisitGtComparatorExpr(GtComparatorExpr* expr, void* target) override;

  std::any VisitLteComparatorExpr(LteComparatorExpr* expr, void* target) override;

  std::any VisitLtComparatorExpr(LtComparatorExpr* expr, void* target) override;

  std::any VisitVar(Var* expr, void* target) override;

  std::any VisitVal(Val* expr, void* target) override;

 private:
  struct AtrributeInfo {
    Type type;
    int32_t index;
  };

  std::unordered_map<std::string, AtrributeInfo> attributes_info_;
  std::vector<std::string> attribute_names_;
};

}  // namespace expression
}  // namespace sdk
}  // namespace dingodb

#endif  // DINGODB_SDK_EXPRESSION_LANGCHAIN_EXPR_ENCODER_H_