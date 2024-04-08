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

#include "sdk/expression/langchain_expr_encoder.h"

#include <any>
#include <cstdint>
#include <iostream>

#include "common/logging.h"
#include "glog/logging.h"
#include "sdk/common/param_config.h"
#include "sdk/expression/coding.h"
#include "sdk/expression/common.h"
#include "sdk/expression/encodes.h"
#include "sdk/expression/langchain_expr.h"
#include "sdk/expression/types.h"

namespace dingodb {
namespace sdk {
namespace expression {

pb::common::CoprocessorV2 LangChainExprEncoder::EncodeToCoprocessor(LangchainExpr* expr) {
  pb::common::CoprocessorV2 coprocessor;

  (*coprocessor.mutable_rel_expr()) = EncodeToFilter(expr);

  // NOTE: get schema must below EncodeToFilter
  auto* schema_wrapper = coprocessor.mutable_original_schema();
  for (int i = 0; i < attribute_names_.size(); i++) {
    const std::string& name = attribute_names_[i];
    auto* schema = schema_wrapper->add_schema();

    auto iter = attributes_info_.find(name);
    CHECK(iter != attributes_info_.end());

    schema->set_type(Type2InternalSchemaTypePB(iter->second.type));
    schema->set_name(name);
    schema->set_index(i);

    coprocessor.add_selection_columns(i);
  }

  VLOG(kSdkVlogLevel) << "langchain expr: " << expr->ToString()
                      << " encode hex string: " << BytesToHexString(coprocessor.rel_expr())
                      << " coprocessor: " << coprocessor.DebugString();
  return coprocessor;
}

std::string LangChainExprEncoder::EncodeToFilter(LangchainExpr* expr) {
  std::string encode;
  encode.append(sizeof(FILTER), FILTER);
  Visit(expr, &encode);
  encode.append(sizeof(EOE), EOE);
  return std::move(encode);
}

std::any LangChainExprEncoder::VisitAndOperatorExpr(AndOperatorExpr* expr, void* target) {
  std::string* dst = static_cast<std::string*>(target);

  Visit(expr->args[0].get(), target);
  for (int i = 1; i < expr->args.size(); i++) {
    std::any tmp = Visit(expr->args[i].get(), target);
    dst->append(sizeof(AND), AND);
  }

  return 0;
}

std::any LangChainExprEncoder::VisitOrOperatorExpr(OrOperatorExpr* expr, void* target) {
  std::string* dst = static_cast<std::string*>(target);

  Visit(expr->args[0].get(), target);
  for (int i = 1; i < expr->args.size(); i++) {
    std::any tmp = Visit(expr->args[i].get(), target);
    dst->append(sizeof(OR), OR);
  }

  return 0;
}

std::any LangChainExprEncoder::VisitNotOperatorExpr(NotOperatorExpr* expr, void* target) {
  //  TODO: check
  Visit(expr->args[0].get(), target);
  std::string* dst = static_cast<std::string*>(target);
  dst->append(sizeof(NOT), NOT);

  return 0;
}

Byte GetEncode(Type type) {
  switch (type) {
    case STRING:
      return TYPE_STRING;
    case DOUBLE:
      return TYPE_DOUBLE;
    case BOOL:
      return TYPE_BOOL;
    case INT64:
      return TYPE_INT64;
    default:
      CHECK(false) << "unknown type: " << static_cast<int>(type);
  }
}

std::any LangChainExprEncoder::VisitEqComparatorExpr(EqComparatorExpr* expr, void* target) {
  Visit(expr->var.get(), target);
  Visit(expr->val.get(), target);

  std::string* dst = static_cast<std::string*>(target);
  dst->append(sizeof(EQ), EQ);

  Byte type_encode = GetEncode(expr->var->type);
  dst->append(sizeof(type_encode), type_encode);

  return 0;
}

std::any LangChainExprEncoder::VisitNeComparatorExpr(NeComparatorExpr* expr, void* target) {
  Visit(expr->var.get(), target);
  Visit(expr->val.get(), target);

  std::string* dst = static_cast<std::string*>(target);
  dst->append(sizeof(NE), NE);

  Byte type_encode = GetEncode(expr->var->type);
  dst->append(sizeof(type_encode), type_encode);

  return 0;
}

std::any LangChainExprEncoder::VisitGteComparatorExpr(GteComparatorExpr* expr, void* target) {
  Visit(expr->var.get(), target);
  Visit(expr->val.get(), target);

  std::string* dst = static_cast<std::string*>(target);
  dst->append(sizeof(GE), GE);

  Byte type_encode = GetEncode(expr->var->type);
  dst->append(sizeof(type_encode), type_encode);

  return 0;
}

std::any LangChainExprEncoder::VisitGtComparatorExpr(GtComparatorExpr* expr, void* target) {
  Visit(expr->var.get(), target);
  Visit(expr->val.get(), target);

  std::string* dst = static_cast<std::string*>(target);
  dst->append(sizeof(GT), GT);

  Byte type_encode = GetEncode(expr->var->type);
  dst->append(sizeof(type_encode), type_encode);

  return 0;
}

std::any LangChainExprEncoder::VisitLteComparatorExpr(LteComparatorExpr* expr, void* target) {
  Visit(expr->var.get(), target);
  Visit(expr->val.get(), target);

  std::string* dst = static_cast<std::string*>(target);
  dst->append(sizeof(LE), LE);

  Byte type_encode = GetEncode(expr->var->type);
  dst->append(sizeof(type_encode), type_encode);

  return 0;
}

std::any LangChainExprEncoder::VisitLtComparatorExpr(LtComparatorExpr* expr, void* target) {
  Visit(expr->var.get(), target);
  Visit(expr->val.get(), target);

  std::string* dst = static_cast<std::string*>(target);
  dst->append(sizeof(LT), LT);

  Byte type_encode = GetEncode(expr->var->type);
  dst->append(sizeof(type_encode), type_encode);

  return 0;
}

std::any LangChainExprEncoder::VisitVar(Var* expr, void* target) {
  int32_t index = 0;
  auto iter = attributes_info_.find(expr->name);
  if (iter != attributes_info_.end()) {
    CHECK_EQ(iter->second.type, expr->type);
    index = iter->second.index;
  } else {
    attribute_names_.push_back(expr->name);
    index = static_cast<int32_t>(attribute_names_.size() - 1);
    attributes_info_.emplace(expr->name, AtrributeInfo{expr->type, index});
  }

  std::string* dst = static_cast<std::string*>(target);
  Byte encode = (VAR | GetEncode(expr->type));
  dst->append(sizeof(encode), encode);

  EncodeVarint(index, dst);

  return 0;
}

static void Encode(const std::string& value, std::string* dst) {
  dst->append(sizeof(Byte), (Byte)(CONST | TYPE_STRING));
  EncodeString(value, dst);
}

static void Encode(double value, std::string* dst) {
  dst->append(sizeof(Byte), (Byte)(CONST | TYPE_DOUBLE));
  EncodeDouble(value, dst);
}

static void Encode(bool value, std::string* dst) {
  if (value) {
    dst->append(sizeof(Byte), (Byte)(CONST | TYPE_BOOL));
  } else {
    dst->append(sizeof(Byte), (Byte)(CONST_N | TYPE_BOOL));
  }
}

static void Encode(int64_t value, std::string* dst) {
  if (value >= 0 || value == INT64_MIN) {
    dst->append(sizeof(Byte), (Byte)(CONST | TYPE_INT64));
    EncodeVarint(value, dst);
  } else {
    dst->append(sizeof(Byte), (Byte)(CONST_N | TYPE_INT64));
    EncodeVarint(-value, dst);
  }
}

std::any LangChainExprEncoder::VisitVal(Val* expr, void* target) {
  std::string* dst = static_cast<std::string*>(target);
  switch (expr->type) {
    case STRING: {
      std::string v = std::any_cast<TypeOf<STRING>>(expr->value);
      Encode(v, dst);
      break;
    }
    case DOUBLE: {
      Encode(std::any_cast<TypeOf<DOUBLE>>(expr->value), dst);
      break;
    }
    case BOOL: {
      Encode(std::any_cast<TypeOf<BOOL>>(expr->value), dst);
      break;
    }
    case INT64: {
      Encode(std::any_cast<TypeOf<INT64>>(expr->value), dst);
      break;
    }
    default:
      CHECK(false) << "unknown type: " << static_cast<int>(expr->type);
  }

  return 0;
}

}  // namespace expression
}  // namespace sdk
}  // namespace dingodb
