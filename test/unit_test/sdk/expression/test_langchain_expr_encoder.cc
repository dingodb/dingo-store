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

#include <gtest/gtest.h>

#include <memory>

#include "sdk/expression/langchain_expr_encoder.h"
#include "sdk/expression/langchain_expr_factory.h"
#include "sdk/status.h"
#include "sdk/utils/codec.h"

namespace dingodb {
namespace sdk {
namespace expression {

class SDKLangChainExprEncoder : public ::testing::Test {
 public:
  void SetUp() override { encoder = std::make_shared<LangChainExprEncoder>(); }

  std::shared_ptr<LangChainExprEncoder> encoder;
};

static Status CreateExpr(const std::string& json_str, std::shared_ptr<LangchainExpr>& expr) {
  LangchainExprFactory expr_factory;
  return expr_factory.CreateExpr(json_str, expr);
}

TEST_F(SDKLangChainExprEncoder, DoubleGt) {
  std::string json_str =
      R"({
          "type": "comparator",
          "comparator": "gt",
          "attribute": "a3",
          "value": 50,
          "value_type": "DOUBLE"
    }
  )";

  std::shared_ptr<LangchainExpr> expr;
  Status s = CreateExpr(json_str, expr);
  EXPECT_TRUE(s.ok());

  std::string bytes = encoder->EncodeToFilter(expr.get());

  EXPECT_EQ(sdk::codec::BytesToHexString(bytes), "713500154049000000000000930500");
}

TEST_F(SDKLangChainExprEncoder, Int64Gt) {
  std::string json_str =
      R"({
          "type": "comparator",
          "comparator": "gt",
          "attribute": "a3",
          "value": 50,
          "value_type": "INT64"
    }
  )";

  std::shared_ptr<LangchainExpr> expr;
  Status s = CreateExpr(json_str, expr);
  EXPECT_TRUE(s.ok());

  std::string bytes = encoder->EncodeToFilter(expr.get());

  EXPECT_EQ(sdk::codec::BytesToHexString(bytes), "7132001232930200");
}

TEST_F(SDKLangChainExprEncoder, StringEq) {
  std::string json_str =
      R"({
          "type": "comparator",
          "comparator": "eq",
          "attribute": "a1",
          "value": "b4",
          "value_type": "STRING"
    }
  )";

  std::shared_ptr<LangchainExpr> expr;
  Status s = CreateExpr(json_str, expr);
  EXPECT_TRUE(s.ok());

  std::string bytes = encoder->EncodeToFilter(expr.get());

  EXPECT_EQ(sdk::codec::BytesToHexString(bytes), "71370017026234910700");
}

TEST_F(SDKLangChainExprEncoder, BoolEqFalse) {
  std::string json_str =
      R"({
          "type": "comparator",
          "comparator": "eq",
          "attribute": "a1",
          "value": false,
          "value_type": "BOOL"
    }
  )";

  std::shared_ptr<LangchainExpr> expr;
  Status s = CreateExpr(json_str, expr);
  EXPECT_TRUE(s.ok());

  std::string bytes = encoder->EncodeToFilter(expr.get());

  EXPECT_EQ(sdk::codec::BytesToHexString(bytes), "71330023910300");
}

TEST_F(SDKLangChainExprEncoder, BoolEqTrue) {
  std::string json_str =
      R"({
          "type": "comparator",
          "comparator": "eq",
          "attribute": "a1",
          "value": true,
          "value_type": "BOOL"
    }
  )";

  std::shared_ptr<LangchainExpr> expr;
  Status s = CreateExpr(json_str, expr);
  EXPECT_TRUE(s.ok());

  std::string bytes = encoder->EncodeToFilter(expr.get());

  EXPECT_EQ(sdk::codec::BytesToHexString(bytes), "71330013910300");
}

TEST_F(SDKLangChainExprEncoder, AndOperator) {
  std::string json_str =
      R"({
      "type": "operator",
      "operator": "and",
      "arguments": [
        {
          "type": "comparator",
          "comparator": "gt",
          "attribute": "a1",
          "value": 4.11,
          "value_type": "DOUBLE"
        },
        {
          "type": "comparator",
          "comparator": "lt",
          "attribute": "a2",
          "value": 6.11,
          "value_type": "DOUBLE"
        },
        {
          "type": "comparator",
          "comparator": "eq",
          "attribute": "a3",
          "value": "b4",
          "value_type": "STRING"
        },
        {
          "type": "comparator",
          "comparator": "eq",
          "attribute": "a4",
          "value": "b4",
          "value_type": "STRING"
        }
      ]
    }
  )";

  std::shared_ptr<LangchainExpr> expr;
  Status s = CreateExpr(json_str, expr);
  EXPECT_TRUE(s.ok());

  std::string bytes = encoder->EncodeToFilter(expr.get());

  EXPECT_EQ(sdk::codec::BytesToHexString(bytes),
            "71350015401070A3D70A3D719305350115401870A3D70A3D7195055237021702623491075237031702623491075200");
}

TEST_F(SDKLangChainExprEncoder, OrOperator) {
  std::string json_str =
      R"({
      "type": "operator",
      "operator": "or",
      "arguments": [
        {
          "type": "comparator",
          "comparator": "eq",
          "attribute": "a1",
          "value": true,
          "value_type": "BOOL"
        },
        {
          "type": "comparator",
          "comparator": "lt",
          "attribute": "a2",
          "value": 6.11,
          "value_type": "DOUBLE"
        }
      ]
    }
  )";

  std::shared_ptr<LangchainExpr> expr;
  Status s = CreateExpr(json_str, expr);
  EXPECT_TRUE(s.ok());

  std::string bytes = encoder->EncodeToFilter(expr.get());

  EXPECT_EQ(sdk::codec::BytesToHexString(bytes), "713300139103350115401870A3D70A3D7195055300");
}

TEST_F(SDKLangChainExprEncoder, NotOperator) {
  std::string json_str =
      R"({
      "type": "operator",
      "operator": "not",
      "arguments": [
        {
          "type": "comparator",
          "comparator": "eq",
          "attribute": "a1",
          "value": true,
          "value_type": "BOOL"
        }
      ]
    }
  )";

  std::shared_ptr<LangchainExpr> expr;
  Status s = CreateExpr(json_str, expr);
  EXPECT_TRUE(s.ok());

  std::string bytes = encoder->EncodeToFilter(expr.get());

  EXPECT_EQ(sdk::codec::BytesToHexString(bytes), "7133001391035100");
}

TEST_F(SDKLangChainExprEncoder, NotOperatorWithOperator) {
  std::string json_str =
      R"({
      "type": "operator",
      "operator": "not",
      "arguments": [
        {
          "type": "operator",
          "operator": "or",
          "arguments": [
            {
              "type": "comparator",
              "comparator": "eq",
              "attribute": "a1",
              "value": true,
              "value_type": "BOOL"
            },
            {
              "type": "comparator",
              "comparator": "lt",
              "attribute": "a2",
              "value": 6.11,
              "value_type": "DOUBLE"
            }
          ]
        }
      ]
    }
  )";

  std::shared_ptr<LangchainExpr> expr;
  Status s = CreateExpr(json_str, expr);
  EXPECT_TRUE(s.ok());

  std::string bytes = encoder->EncodeToFilter(expr.get());

  EXPECT_EQ(sdk::codec::BytesToHexString(bytes), "713300139103350115401870A3D70A3D719505535100");
}

// the second comparator is ignored
TEST_F(SDKLangChainExprEncoder, NotOperatorWithNestedComparator) {
  std::string json_str =
      R"({
      "type": "operator",
      "operator": "not",
      "arguments": [
        {
          "type": "comparator",
          "comparator": "eq",
          "attribute": "a1",
          "value": 4.11,
          "value_type": "DOUBLE"
        },
        {
          "type": "comparator",
          "comparator": "eq",
          "attribute": "a3",
          "value": "b4",
          "value_type": "STRING"
        }
      ]
    }
  )";

  std::shared_ptr<LangchainExpr> expr;
  Status s = CreateExpr(json_str, expr);
  EXPECT_TRUE(s.ok());

  std::string bytes = encoder->EncodeToFilter(expr.get());

  EXPECT_EQ(sdk::codec::BytesToHexString(bytes), "71350015401070A3D70A3D7191055100");
}

}  // namespace expression
}  // namespace sdk
}  // namespace dingodb
