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

#ifndef DINGODB_SDK_EXPRESSION_LANGCHAIN_EXPR_H_
#define DINGODB_SDK_EXPRESSION_LANGCHAIN_EXPR_H_

#include <any>
#include <cstdint>
#include <memory>
#include <sstream>
#include <string>
#include <variant>
#include <vector>

#include "common/logging.h"
#include "sdk/expression/types.h"

namespace dingodb {
namespace sdk {
namespace expression {

class LangchainExprVisitor;

enum OperatorType : uint8_t { kAnd, kOr, kNot };

std::string OperatorTypeToString(OperatorType operator_type);

enum ComparatorType : uint8_t { kEq, kNe, kGte, kGt, kLte, kLt };

std::string ComparatorTypeToString(ComparatorType comparator_type);

class ComparatorExpr;
class Var;
class Val;

class LangchainExpr {
 public:
  LangchainExpr() = default;
  virtual ~LangchainExpr() = default;

  virtual std::any Accept(LangchainExprVisitor* visitor, void* target) = 0;

  virtual std::string ToString() const = 0;
};

class OperatorExpr : public LangchainExpr {
 public:
  OperatorExpr(OperatorType operator_type) : operator_type(operator_type) {}

  ~OperatorExpr() override = default;

  std::string ToString() const override;

  virtual void AddArgument(std::shared_ptr<LangchainExpr> arg) { args.push_back(arg); }

  const OperatorType operator_type;

  std::vector<std::shared_ptr<LangchainExpr>> args;
};

class AndOperatorExpr : public OperatorExpr {
 public:
  AndOperatorExpr() : OperatorExpr(OperatorType::kAnd) {}
  ~AndOperatorExpr() override = default;

  std::any Accept(LangchainExprVisitor* visitor, void* target) override;
};

class OrOperatorExpr : public OperatorExpr {
 public:
  OrOperatorExpr() : OperatorExpr(OperatorType::kOr) {}
  ~OrOperatorExpr() override = default;

  std::any Accept(LangchainExprVisitor* visitor, void* target) override;
};

class NotOperatorExpr : public OperatorExpr {
 public:
  NotOperatorExpr() : OperatorExpr(OperatorType::kNot) {}
  ~NotOperatorExpr() override = default;

  std::any Accept(LangchainExprVisitor* visitor, void* target) override;

  void AddArgument(std::shared_ptr<LangchainExpr> arg) override {
    if (args.empty()) {
      args.push_back(arg);
    } else {
      DINGO_LOG(INFO) << "NotOperatorExpr can only have one argument. ignore " << arg->ToString();
    }
  }
};

class ComparatorExpr : public LangchainExpr {
 public:
  ComparatorExpr(ComparatorType comparator_type) : comparator_type(comparator_type) {}
  ~ComparatorExpr() override = default;

  std::string ToString() const override;

  const ComparatorType comparator_type;
  std::shared_ptr<Var> var;
  std::shared_ptr<Val> val;
};

class EqComparatorExpr : public ComparatorExpr {
 public:
  EqComparatorExpr() : ComparatorExpr(ComparatorType::kEq) {}
  ~EqComparatorExpr() override = default;

  std::any Accept(LangchainExprVisitor* visitor, void* target) override;
};

class NeComparatorExpr : public ComparatorExpr {
 public:
  NeComparatorExpr() : ComparatorExpr(ComparatorType::kNe) {}
  ~NeComparatorExpr() override = default;

  std::any Accept(LangchainExprVisitor* visitor, void* target) override;
};

class GteComparatorExpr : public ComparatorExpr {
 public:
  GteComparatorExpr() : ComparatorExpr(ComparatorType::kGte) {}
  ~GteComparatorExpr() override = default;

  std::any Accept(LangchainExprVisitor* visitor, void* target) override;
};

class GtComparatorExpr : public ComparatorExpr {
 public:
  GtComparatorExpr() : ComparatorExpr(ComparatorType::kGt) {}

  ~GtComparatorExpr() override = default;

  std::any Accept(LangchainExprVisitor* visitor, void* target) override;
};

class LteComparatorExpr : public ComparatorExpr {
 public:
  LteComparatorExpr() : ComparatorExpr(ComparatorType::kLte) {}
  ~LteComparatorExpr() override = default;

  std::any Accept(LangchainExprVisitor* visitor, void* target) override;
};

class LtComparatorExpr : public ComparatorExpr {
 public:
  LtComparatorExpr() : ComparatorExpr(ComparatorType::kLt) {}
  ~LtComparatorExpr() override = default;

  std::any Accept(LangchainExprVisitor* visitor, void* target) override;
};

class Var : public LangchainExpr {
 public:
  Var(std::string name, Type type) : name(std::move(name)), type(type) {}

  ~Var() override = default;

  std::any Accept(LangchainExprVisitor* visitor, void* target) override;

  std::string ToString() const override;

  std::string name;
  Type type;
};

class Val : public LangchainExpr {
 public:
  Val(std::any value, Type type) : value(value), type(type) {}

  ~Val() override = default;

  std::any Accept(LangchainExprVisitor* visitor, void* target) override;

  std::string ToString() const override;

  std::any value;
  Type type;
};

}  // namespace expression
}  // namespace sdk

}  // namespace dingodb

#endif  // DINGODB_SDK_EXPRESSION_LANGCHAIN_EXPR_H_