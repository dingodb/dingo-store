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

#include "sdk/expression/langchain_expr_factory.h"

#include <nlohmann/json.hpp>

#include "sdk/expression/langchain_expr.h"
#include "sdk/status.h"

namespace dingodb {
namespace sdk {
namespace expression {

namespace {
Status CreateOperatorExpr(const nlohmann::json& j, std::shared_ptr<LangchainExpr>& expr) {
  std::shared_ptr<OperatorExpr> tmp;

  std::string operator_type = j.at("operator");
  if (operator_type == "and") {
    tmp = std::make_shared<AndOperatorExpr>();
  } else if (operator_type == "or") {
    tmp = std::make_shared<OrOperatorExpr>();
  } else if (operator_type == "not") {
    tmp = std::make_shared<NotOperatorExpr>();
  } else {
    return Status::InvalidArgument("Unknown operator type: " + operator_type);
  }

  for (const auto& arg : j.at("arguments")) {
    std::string json_str = arg.dump();

    std::shared_ptr<LangchainExpr> sub_expr;
    DINGO_RETURN_NOT_OK(LangchainExprFactory::CreateExpr(json_str, sub_expr));
    tmp->AddArgument(sub_expr);
  }

  expr = std::move(tmp);
  return Status::OK();
}

Status CreateComparatorExpr(const nlohmann::json& j, std::shared_ptr<LangchainExpr>& expr) {
  std::shared_ptr<ComparatorExpr> tmp;

  std::string comparator_type = j.at("comparator");
  if (comparator_type == "eq") {
    tmp = std::make_shared<EqComparatorExpr>();
  } else if (comparator_type == "ne") {
    tmp = std::make_shared<NeComparatorExpr>();
  } else if (comparator_type == "lt") {
    tmp = std::make_shared<LtComparatorExpr>();
  } else if (comparator_type == "lte") {
    tmp = std::make_shared<LteComparatorExpr>();
  } else if (comparator_type == "gt") {
    tmp = std::make_shared<GtComparatorExpr>();
  } else if (comparator_type == "gte") {
    tmp = std::make_shared<GteComparatorExpr>();
  } else {
    return Status::InvalidArgument("Unknown comparator type: " + comparator_type);
  }

  std::string name = j.at("attribute");

  std::string value_type = j.at("value_type");
  if (value_type == "STRING") {
    tmp->var = std::make_shared<Var>(std::move(name), STRING);
    tmp->val = std::make_shared<Val>(j.at("value").get<std::string>(), STRING);
  } else if (value_type == "INT64") {
    tmp->var = std::make_shared<Var>(std::move(name), INT64);
    tmp->val = std::make_shared<Val>(j.at("value").get<int64_t>(), INT64);
  } else if (value_type == "DOUBLE") {
    tmp->var = std::make_shared<Var>(std::move(name), DOUBLE);
    tmp->val = std::make_shared<Val>(j.at("value").get<double>(), DOUBLE);
  } else if (value_type == "BOOL") {
    tmp->var = std::make_shared<Var>(std::move(name), BOOL);
    tmp->val = std::make_shared<Val>(j.at("value").get<bool>(), BOOL);
  } else {
    return Status::InvalidArgument("Unknown value type: " + value_type);
  }

  expr = std::move(tmp);
  return Status::OK();
}

}  // namespace

Status LangchainExprFactory::CreateExpr(const std::string& expr_json_str, std::shared_ptr<LangchainExpr>& expr) {
  std::shared_ptr<LangchainExpr> tmp;

  nlohmann::json j = nlohmann::json::parse(expr_json_str);
  std::string type = j.at("type");
  if (type == "operator") {
    DINGO_RETURN_NOT_OK(CreateOperatorExpr(j, tmp));
  } else if (type == "comparator") {
    DINGO_RETURN_NOT_OK(CreateComparatorExpr(j, tmp));
  } else {
    return Status::InvalidArgument("Unknown expression type: " + type);
  }

  expr = std::move(tmp);

  return Status::OK();
}
}  // namespace expression

}  // namespace sdk

}  // namespace dingodb