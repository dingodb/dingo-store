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
#include <utility>

#include "fmt/core.h"
#include "sdk/common/param_config.h"
#include "sdk/expression/langchain_expr.h"
#include "sdk/status.h"
#include "sdk/types.h"
#include "sdk/types_util.h"

namespace dingodb {
namespace sdk {
namespace expression {

namespace {
Status CreateOperatorExpr(LangchainExprFactory* expr_factory, const nlohmann::json& j,
                          std::shared_ptr<LangchainExpr>& expr) {
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
    DINGO_RETURN_NOT_OK(expr_factory->CreateExpr(json_str, sub_expr));
    tmp->AddArgument(sub_expr);
  }

  expr = std::move(tmp);
  return Status::OK();
}

Status CreateComparatorExpr(LangchainExprFactory* expr_factory, const nlohmann::json& j,
                            std::shared_ptr<LangchainExpr>& expr) {
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
  Type type;
  if (value_type == "STRING") {
    type = kSTRING;
  } else if (value_type == "INT64") {
    type = kINT64;
  } else if (value_type == "DOUBLE") {
    type = kDOUBLE;
  } else if (value_type == "BOOL") {
    type = kBOOL;
  } else {
    return Status::InvalidArgument("Unknown value type: " + value_type);
  }

  DINGO_RETURN_NOT_OK(expr_factory->MaybeRemapType(name, type));

  switch (type) {
    case kBOOL:
      tmp->var = std::make_shared<Var>(name, kBOOL);
      tmp->val = std::make_shared<Val>(name, kBOOL, j.at("value").get<TypeOf<kBOOL>>());
      break;
    case kINT64:
      tmp->var = std::make_shared<Var>(name, kINT64);
      tmp->val = std::make_shared<Val>(name, kINT64, j.at("value").get<TypeOf<kINT64>>());
      break;
    case kDOUBLE:
      tmp->var = std::make_shared<Var>(name, kDOUBLE);
      tmp->val = std::make_shared<Val>(name, kDOUBLE, j.at("value").get<TypeOf<kDOUBLE>>());
      break;
    case kSTRING:
      tmp->var = std::make_shared<Var>(name, kSTRING);
      tmp->val = std::make_shared<Val>(name, kSTRING, j.at("value").get<TypeOf<kSTRING>>());
      break;
    default:
      CHECK(false) << "Unknown value type: " << value_type;
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
    DINGO_RETURN_NOT_OK(CreateOperatorExpr(this, j, tmp));
  } else if (type == "comparator") {
    DINGO_RETURN_NOT_OK(CreateComparatorExpr(this, j, tmp));
  } else {
    return Status::InvalidArgument("Unknown expression type: " + type);
  }

  expr = std::move(tmp);

  VLOG(kSdkVlogLevel) << "expr_json_str: " << expr_json_str << " expr: " << expr->ToString();
  return Status::OK();
}

Status LangchainExprFactory::MaybeRemapType(const std::string& name, Type& type) {
  (void)name;
  (void)type;
  return Status::OK();
}

SchemaLangchainExprFactory::SchemaLangchainExprFactory(const pb::common::ScalarSchema& schema) {
  for (const auto& schema_item : schema.fields()) {
    CHECK(attribute_type_
              .insert(std::make_pair(schema_item.key(), InternalScalarFieldTypePB2Type(schema_item.field_type())))
              .second);
  }
}

Status SchemaLangchainExprFactory::MaybeRemapType(const std::string& name, Type& type) {
  auto iter = attribute_type_.find(name);
  if (iter != attribute_type_.end()) {
    Type schema_type = iter->second;
    if (kTypeConversionMatrix[type][schema_type]) {
      type = schema_type;
    } else {
      std::string err_msg = fmt::format("attribute: {}, type: {}, can't convert to schema type: {}", name,
                                        TypeToString(type), TypeToString(schema_type));
      DINGO_LOG(WARNING) << err_msg;
      return Status::InvalidArgument(err_msg);
    }
  } else {
    // TODO: if not found in schema, should we return not ok?
    DINGO_LOG(INFO) << "attribute: " << name << " type:" << TypeToString(type) << " not found in schema";
  }

  return Status::OK();
}

}  // namespace expression

}  // namespace sdk

}  // namespace dingodb