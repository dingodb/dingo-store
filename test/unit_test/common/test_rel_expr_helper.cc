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

#include <cxxabi.h>
#include <dirent.h>
#include <gtest/gtest.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <any>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <exception>
#include <iostream>
#include <memory>
#include <optional>
#include <string>
#include <type_traits>
#include <variant>
#include <vector>

#include "butil/status.h"
#include "common/logging.h"
#include "coprocessor/rel_expr_helper.h"
#include "coprocessor/utils.h"
#include "proto/common.pb.h"
#include "proto/error.pb.h"
#include "serial/schema/base_schema.h"
#include "serial/schema/boolean_schema.h"
#include "serial/schema/double_schema.h"
#include "serial/schema/float_schema.h"
#include "serial/schema/integer_schema.h"
#include "serial/schema/long_schema.h"
#include "serial/schema/string_schema.h"

namespace dingodb {

class RelExprHelperTest : public testing::Test {
 protected:
  static void SetUpTestSuite() {}

  static void TearDownTestSuite() {}

  void SetUp() override {}

  void TearDown() override {}
};

TEST_F(RelExprHelperTest, TransToOperand) {
  butil::Status ok;

  // invalid column
  {
    BaseSchema::Type type;
    std::any column;
    std::unique_ptr<std::vector<expr::Operand>> operand_ptr;
    ok = RelExprHelper::TransToOperand(type, column, operand_ptr);
    EXPECT_EQ(ok.error_code(), pb::error::EILLEGAL_PARAMTETERS);
  }

  // unique_ptr not init
  {
    BaseSchema::Type type = BaseSchema::Type::kBool;
    std::any column = std::optional<bool>(nullptr);
    std::unique_ptr<std::vector<expr::Operand>> operand_ptr;
    ok = RelExprHelper::TransToOperand(type, column, operand_ptr);
    EXPECT_EQ(ok.error_code(), pb::error::EILLEGAL_PARAMTETERS);
  }

  // type bool
  {
    std::unique_ptr<std::vector<expr::Operand>> operand_ptr = std::make_unique<std::vector<expr::Operand>>();
    BaseSchema::Type type = BaseSchema::Type::kBool;
    std::any column;
    ok = RelExprHelper::TransToOperand(type, column, operand_ptr);
    EXPECT_EQ(ok.error_code(), pb::error::EILLEGAL_PARAMTETERS);

    column = std::optional<bool>(std::nullopt);
    ok = RelExprHelper::TransToOperand(type, column, operand_ptr);
    EXPECT_EQ(ok.error_code(), pb::error::OK);

    column = std::optional<bool>(false);
    ok = RelExprHelper::TransToOperand(type, column, operand_ptr);
    EXPECT_EQ(ok.error_code(), pb::error::OK);
  }

  // type int
  {
    std::unique_ptr<std::vector<expr::Operand>> operand_ptr = std::make_unique<std::vector<expr::Operand>>();
    BaseSchema::Type type = BaseSchema::Type::kInteger;
    std::any column;
    ok = RelExprHelper::TransToOperand(type, column, operand_ptr);
    EXPECT_EQ(ok.error_code(), pb::error::EILLEGAL_PARAMTETERS);

    column = std::optional<int32_t>(std::nullopt);
    ok = RelExprHelper::TransToOperand(type, column, operand_ptr);
    EXPECT_EQ(ok.error_code(), pb::error::OK);

    column = std::optional<int32_t>(1);
    ok = RelExprHelper::TransToOperand(type, column, operand_ptr);
    EXPECT_EQ(ok.error_code(), pb::error::OK);
  }

  // type float
  {
    std::unique_ptr<std::vector<expr::Operand>> operand_ptr = std::make_unique<std::vector<expr::Operand>>();
    BaseSchema::Type type = BaseSchema::Type::kFloat;
    std::any column;
    ok = RelExprHelper::TransToOperand(type, column, operand_ptr);
    EXPECT_EQ(ok.error_code(), pb::error::EILLEGAL_PARAMTETERS);

    column = std::optional<float>(std::nullopt);
    ok = RelExprHelper::TransToOperand(type, column, operand_ptr);
    EXPECT_EQ(ok.error_code(), pb::error::OK);

    column = std::optional<float>(1.23f);
    ok = RelExprHelper::TransToOperand(type, column, operand_ptr);
    EXPECT_EQ(ok.error_code(), pb::error::OK);
  }

  // type int64_t
  {
    std::unique_ptr<std::vector<expr::Operand>> operand_ptr = std::make_unique<std::vector<expr::Operand>>();
    BaseSchema::Type type = BaseSchema::Type::kLong;
    std::any column;
    ok = RelExprHelper::TransToOperand(type, column, operand_ptr);
    EXPECT_EQ(ok.error_code(), pb::error::EILLEGAL_PARAMTETERS);

    column = std::optional<int64_t>(std::nullopt);
    ok = RelExprHelper::TransToOperand(type, column, operand_ptr);
    EXPECT_EQ(ok.error_code(), pb::error::OK);

    column = std::optional<int64_t>(1);
    ok = RelExprHelper::TransToOperand(type, column, operand_ptr);
    EXPECT_EQ(ok.error_code(), pb::error::OK);
  }

  // type double
  {
    std::unique_ptr<std::vector<expr::Operand>> operand_ptr = std::make_unique<std::vector<expr::Operand>>();
    BaseSchema::Type type = BaseSchema::Type::kDouble;
    std::any column;
    ok = RelExprHelper::TransToOperand(type, column, operand_ptr);
    EXPECT_EQ(ok.error_code(), pb::error::EILLEGAL_PARAMTETERS);

    column = std::optional<double>(std::nullopt);
    ok = RelExprHelper::TransToOperand(type, column, operand_ptr);
    EXPECT_EQ(ok.error_code(), pb::error::OK);

    column = std::optional<double>(1.23);
    ok = RelExprHelper::TransToOperand(type, column, operand_ptr);
    EXPECT_EQ(ok.error_code(), pb::error::OK);
  }

  // type string
  {
    std::unique_ptr<std::vector<expr::Operand>> operand_ptr = std::make_unique<std::vector<expr::Operand>>();
    BaseSchema::Type type = BaseSchema::Type::kString;
    std::any column;
    ok = RelExprHelper::TransToOperand(type, column, operand_ptr);
    EXPECT_EQ(ok.error_code(), pb::error::EILLEGAL_PARAMTETERS);

    column = std::optional<std::shared_ptr<std::string>>(std::nullopt);
    ok = RelExprHelper::TransToOperand(type, column, operand_ptr);
    EXPECT_EQ(ok.error_code(), pb::error::OK);

    column = std::optional<std::shared_ptr<std::string>>(std::make_shared<std::string>("123"));
    ok = RelExprHelper::TransToOperand(type, column, operand_ptr);
    EXPECT_EQ(ok.error_code(), pb::error::OK);
  }

  // type kBoolList
  {
    std::unique_ptr<std::vector<expr::Operand>> operand_ptr = std::make_unique<std::vector<expr::Operand>>();
    BaseSchema::Type type = BaseSchema::Type::kBoolList;
    std::any column;
    ok = RelExprHelper::TransToOperand(type, column, operand_ptr);
    EXPECT_EQ(ok.error_code(), pb::error::EILLEGAL_PARAMTETERS);

    column = std::optional<std::shared_ptr<std::vector<bool>>>(std::nullopt);
    ok = RelExprHelper::TransToOperand(type, column, operand_ptr);
    EXPECT_EQ(ok.error_code(), pb::error::OK);

    column = std::optional<std::shared_ptr<std::vector<bool>>>(std::make_shared<std::vector<bool>>());
    ok = RelExprHelper::TransToOperand(type, column, operand_ptr);
    EXPECT_EQ(ok.error_code(), pb::error::OK);
  }

  // type kIntegerList
  {
    std::unique_ptr<std::vector<expr::Operand>> operand_ptr = std::make_unique<std::vector<expr::Operand>>();
    BaseSchema::Type type = BaseSchema::Type::kIntegerList;
    std::any column;
    ok = RelExprHelper::TransToOperand(type, column, operand_ptr);
    EXPECT_EQ(ok.error_code(), pb::error::EILLEGAL_PARAMTETERS);

    column = std::optional<std::shared_ptr<std::vector<int32_t>>>(std::nullopt);
    ok = RelExprHelper::TransToOperand(type, column, operand_ptr);
    EXPECT_EQ(ok.error_code(), pb::error::OK);

    column = std::optional<std::shared_ptr<std::vector<int32_t>>>();
    ok = RelExprHelper::TransToOperand(type, column, operand_ptr);
    EXPECT_EQ(ok.error_code(), pb::error::OK);
  }

  // type kFloatList
  {
    std::unique_ptr<std::vector<expr::Operand>> operand_ptr = std::make_unique<std::vector<expr::Operand>>();
    BaseSchema::Type type = BaseSchema::Type::kFloatList;
    std::any column;
    ok = RelExprHelper::TransToOperand(type, column, operand_ptr);
    EXPECT_EQ(ok.error_code(), pb::error::EILLEGAL_PARAMTETERS);

    column = std::optional<std::shared_ptr<std::vector<float>>>(std::nullopt);
    ok = RelExprHelper::TransToOperand(type, column, operand_ptr);
    EXPECT_EQ(ok.error_code(), pb::error::OK);

    column = std::optional<std::shared_ptr<std::vector<float>>>();
    ok = RelExprHelper::TransToOperand(type, column, operand_ptr);
    EXPECT_EQ(ok.error_code(), pb::error::OK);
  }

  // type kLongList
  {
    std::unique_ptr<std::vector<expr::Operand>> operand_ptr = std::make_unique<std::vector<expr::Operand>>();
    BaseSchema::Type type = BaseSchema::Type::kLongList;
    std::any column;
    ok = RelExprHelper::TransToOperand(type, column, operand_ptr);
    EXPECT_EQ(ok.error_code(), pb::error::EILLEGAL_PARAMTETERS);

    column = std::optional<std::shared_ptr<std::vector<int64_t>>>(std::nullopt);
    ok = RelExprHelper::TransToOperand(type, column, operand_ptr);
    EXPECT_EQ(ok.error_code(), pb::error::OK);

    column = std::optional<std::shared_ptr<std::vector<int64_t>>>();
    ok = RelExprHelper::TransToOperand(type, column, operand_ptr);
    EXPECT_EQ(ok.error_code(), pb::error::OK);
  }

  // type kDoubleList
  {
    std::unique_ptr<std::vector<expr::Operand>> operand_ptr = std::make_unique<std::vector<expr::Operand>>();
    BaseSchema::Type type = BaseSchema::Type::kDoubleList;
    std::any column;
    ok = RelExprHelper::TransToOperand(type, column, operand_ptr);
    EXPECT_EQ(ok.error_code(), pb::error::EILLEGAL_PARAMTETERS);

    column = std::optional<std::shared_ptr<std::vector<double>>>(std::nullopt);
    ok = RelExprHelper::TransToOperand(type, column, operand_ptr);
    EXPECT_EQ(ok.error_code(), pb::error::OK);

    column = std::optional<std::shared_ptr<std::vector<double>>>();
    ok = RelExprHelper::TransToOperand(type, column, operand_ptr);
    EXPECT_EQ(ok.error_code(), pb::error::OK);
  }

  // type kStringList
  {
    std::unique_ptr<std::vector<expr::Operand>> operand_ptr = std::make_unique<std::vector<expr::Operand>>();
    BaseSchema::Type type = BaseSchema::Type::kStringList;
    std::any column;
    ok = RelExprHelper::TransToOperand(type, column, operand_ptr);
    EXPECT_EQ(ok.error_code(), pb::error::EILLEGAL_PARAMTETERS);

    column = std::optional<std::shared_ptr<std::vector<std::string>>>(std::nullopt);
    ok = RelExprHelper::TransToOperand(type, column, operand_ptr);
    EXPECT_EQ(ok.error_code(), pb::error::OK);

    column = std::optional<std::shared_ptr<std::vector<std::string>>>();
    ok = RelExprHelper::TransToOperand(type, column, operand_ptr);
    EXPECT_EQ(ok.error_code(), pb::error::OK);
  }

  // type all
  {
    std::unique_ptr<std::vector<expr::Operand>> operand_ptr = std::make_unique<std::vector<expr::Operand>>();
    BaseSchema::Type type = BaseSchema::Type::kBool;
    std::any column;

    column = std::optional<bool>(std::nullopt);
    ok = RelExprHelper::TransToOperand(type, column, operand_ptr);
    EXPECT_EQ(ok.error_code(), pb::error::OK);

    column = std::optional<bool>(false);
    ok = RelExprHelper::TransToOperand(type, column, operand_ptr);
    EXPECT_EQ(ok.error_code(), pb::error::OK);

    type = BaseSchema::Type::kInteger;
    column = std::optional<int32_t>(std::nullopt);
    ok = RelExprHelper::TransToOperand(type, column, operand_ptr);
    EXPECT_EQ(ok.error_code(), pb::error::OK);

    column = std::optional<int32_t>(1);
    ok = RelExprHelper::TransToOperand(type, column, operand_ptr);
    EXPECT_EQ(ok.error_code(), pb::error::OK);

    type = BaseSchema::Type::kFloat;
    column = std::optional<float>(std::nullopt);
    ok = RelExprHelper::TransToOperand(type, column, operand_ptr);
    EXPECT_EQ(ok.error_code(), pb::error::OK);

    column = std::optional<float>(1.23f);
    ok = RelExprHelper::TransToOperand(type, column, operand_ptr);
    EXPECT_EQ(ok.error_code(), pb::error::OK);

    type = BaseSchema::Type::kLong;
    column = std::optional<int64_t>(std::nullopt);
    ok = RelExprHelper::TransToOperand(type, column, operand_ptr);
    EXPECT_EQ(ok.error_code(), pb::error::OK);

    column = std::optional<int64_t>(1);
    ok = RelExprHelper::TransToOperand(type, column, operand_ptr);
    EXPECT_EQ(ok.error_code(), pb::error::OK);

    type = BaseSchema::Type::kDouble;
    column = std::optional<double>(std::nullopt);
    ok = RelExprHelper::TransToOperand(type, column, operand_ptr);
    EXPECT_EQ(ok.error_code(), pb::error::OK);

    column = std::optional<double>(1.23);
    ok = RelExprHelper::TransToOperand(type, column, operand_ptr);
    EXPECT_EQ(ok.error_code(), pb::error::OK);

    type = BaseSchema::Type::kString;
    column = std::optional<std::shared_ptr<std::string>>(std::nullopt);
    ok = RelExprHelper::TransToOperand(type, column, operand_ptr);
    EXPECT_EQ(ok.error_code(), pb::error::OK);

    column = std::optional<std::shared_ptr<std::string>>(std::make_shared<std::string>("123"));
    ok = RelExprHelper::TransToOperand(type, column, operand_ptr);
    EXPECT_EQ(ok.error_code(), pb::error::OK);

    type = BaseSchema::Type::kBoolList;
    std::vector<bool> bool_list{false, true, false};
    column = std::optional<std::shared_ptr<std::vector<bool>>>(std::make_shared<std::vector<bool>>(bool_list));
    ok = RelExprHelper::TransToOperand(type, column, operand_ptr);
    EXPECT_EQ(ok.error_code(), pb::error::OK);

    type = BaseSchema::Type::kIntegerList;
    std::vector<int32_t> int32_list{1, 2, 3};
    column = std::optional<std::shared_ptr<std::vector<int32_t>>>(std::make_shared<std::vector<int32_t>>(int32_list));
    ok = RelExprHelper::TransToOperand(type, column, operand_ptr);
    EXPECT_EQ(ok.error_code(), pb::error::OK);

    type = BaseSchema::Type::kFloatList;
    std::vector<float> float_list{1.23f, 2.23f, 3.23f};
    column = std::optional<std::shared_ptr<std::vector<float>>>(std::make_shared<std::vector<float>>(float_list));
    ok = RelExprHelper::TransToOperand(type, column, operand_ptr);
    EXPECT_EQ(ok.error_code(), pb::error::OK);

    type = BaseSchema::Type::kLongList;
    std::vector<int64_t> int64_list{1000, 2000, 3000};
    column = std::optional<std::shared_ptr<std::vector<int64_t>>>(std::make_shared<std::vector<int64_t>>(int64_list));
    ok = RelExprHelper::TransToOperand(type, column, operand_ptr);
    EXPECT_EQ(ok.error_code(), pb::error::OK);

    type = BaseSchema::Type::kDoubleList;
    std::vector<double> double_list{10000.23f, 200000.23f, 300000.23f};
    column = std::optional<std::shared_ptr<std::vector<double>>>(std::make_shared<std::vector<double>>(double_list));
    ok = RelExprHelper::TransToOperand(type, column, operand_ptr);
    EXPECT_EQ(ok.error_code(), pb::error::OK);

    type = BaseSchema::Type::kStringList;
    std::vector<std::string> string_list{"vvvvvvv", "wwwwwwww", "yyyyyyyy"};
    column = std::optional<std::shared_ptr<std::vector<std::string>>>(
        std::make_shared<std::vector<std::string>>(string_list));
    ok = RelExprHelper::TransToOperand(type, column, operand_ptr);
    EXPECT_EQ(ok.error_code(), pb::error::OK);

    for (const auto &operand : *operand_ptr) {
      try {
        auto v = operand.GetValue<bool>();
        LOG(INFO) << "bool : " << v;
        continue;
      } catch (const std::exception &e) {
        LOG(INFO) << "exception : " << e.what();
      }

      try {
        auto v = operand.GetValue<int>();
        LOG(INFO) << "int : " << v;
        continue;
      } catch (const std::exception &e) {
        LOG(INFO) << "exception : " << e.what();
      }

      try {
        auto v = operand.GetValue<float>();
        LOG(INFO) << "float : " << v;
        continue;
      } catch (const std::exception &e) {
        LOG(INFO) << "exception : " << e.what();
      }

      try {
        auto v = operand.GetValue<int64_t>();
        LOG(INFO) << "int64_t : " << v;
        continue;
      } catch (const std::exception &e) {
        LOG(INFO) << "exception : " << e.what();
      }

      try {
        auto v = operand.GetValue<double>();
        LOG(INFO) << "double : " << v;
        continue;
      } catch (const std::exception &e) {
        LOG(INFO) << "exception : " << e.what();
      }

      try {
        auto v = operand.GetValue<dingodb::expr::String>();
        LOG(INFO) << "std::shared_ptr<std::string> : " << *v;
        continue;
      } catch (const std::exception &e) {
        LOG(INFO) << "exception : " << e.what();
      }

      try {
        auto v = operand.GetValue<std::monostate>();
        LOG(INFO) << "std::monostate : "
                  << "nullptr";
        continue;
      } catch (const std::exception &e) {
        LOG(INFO) << "exception : " << e.what();
      }

      auto lambda_show_function = [](const std::string &name, auto v) {
        LOG(INFO) << name;
        if (v) {
          for (const auto &elem : *v) {
            LOG(INFO) << elem << ", ";
          }
        }
        LOG(INFO) << "\n";
      };

      try {
        auto v = operand.GetValue<std::shared_ptr<std::vector<bool>>>();
        lambda_show_function("std::shared_ptr<std::vector<bool>> : ", v);
        continue;
      } catch (const std::exception &e) {
        LOG(INFO) << "exception : " << e.what();
      }

      try {
        auto v = operand.GetValue<std::shared_ptr<std::vector<int32_t>>>();
        lambda_show_function("std::shared_ptr<std::vector<int32_t>> : ", v);
        continue;
      } catch (const std::exception &e) {
        LOG(INFO) << "exception : " << e.what();
      }

      try {
        auto v = operand.GetValue<std::shared_ptr<std::vector<int64_t>>>();
        lambda_show_function("std::shared_ptr<std::vector<int64_t>> : ", v);
        continue;
      } catch (const std::exception &e) {
        LOG(INFO) << "exception : " << e.what();
      }

      try {
        auto v = operand.GetValue<std::shared_ptr<std::vector<float>>>();
        lambda_show_function("std::shared_ptr<std::vector<float>> : ", v);
        continue;
      } catch (const std::exception &e) {
        LOG(INFO) << "exception : " << e.what();
      }

      try {
        auto v = operand.GetValue<std::shared_ptr<std::vector<double>>>();
        lambda_show_function("std::shared_ptr<std::vector<double>> : ", v);
        continue;
      } catch (const std::exception &e) {
        LOG(INFO) << "exception : " << e.what();
      }

      try {
        auto v = operand.GetValue<std::shared_ptr<std::vector<std::string>>>();
        lambda_show_function("std::shared_ptr<std::vector<std::string>> : ", v);
        continue;
      } catch (const std::exception &e) {
        LOG(INFO) << "exception : " << e.what();
      }
    }
  }
}

TEST_F(RelExprHelperTest, TransFromOperand) {
  butil::Status ok;

  // invalid param
  {
    BaseSchema::Type type;
    std::unique_ptr<std::vector<expr::Operand>> operand_ptr;
    size_t index;
    std::vector<std::any> columns;
    ok = RelExprHelper::TransFromOperand(type, operand_ptr, index, columns);
    EXPECT_EQ(ok.error_code(), pb::error::EILLEGAL_PARAMTETERS);
  }

  // invalid param
  {
    std::unique_ptr<std::vector<expr::Operand>> operand_ptr = std::make_unique<std::vector<expr::Operand>>();
    BaseSchema::Type type = BaseSchema::Type::kBool;
    std::any column;

    column = std::optional<bool>(false);
    ok = RelExprHelper::TransToOperand(type, column, operand_ptr);
    EXPECT_EQ(ok.error_code(), pb::error::OK);

    type = BaseSchema::Type::kInteger;
    size_t index = 0;
    std::vector<std::any> columns;
    ok = RelExprHelper::TransFromOperand(type, operand_ptr, index, columns);
    EXPECT_EQ(ok.error_code(), pb::error::EILLEGAL_PARAMTETERS);
  }

  auto call_trans_from_operand_lambda = [](BaseSchema::Type type,
                                           const std::unique_ptr<std::vector<expr::Operand>> &operand_ptr, auto t) {
    butil::Status ok;
    size_t index = 0;
    std::vector<std::any> columns;

    for (const auto &opera : *operand_ptr) {
      ok = RelExprHelper::TransFromOperand(type, operand_ptr, index, columns);
      EXPECT_EQ(ok.error_code(), pb::error::OK);
      index++;
    }

    // ok = RelExprHelper::TransFromOperand(type, operand_ptr, index, columns);
    // EXPECT_EQ(ok.error_code(), pb::error::OK);
    // index++;
    // ok = RelExprHelper::TransFromOperand(type, operand_ptr, index, columns);
    // EXPECT_EQ(ok.error_code(), pb::error::OK);

    for (const auto &column : columns) {
      const auto &value = std::any_cast<std::optional<decltype(t)>>(column);
      if (value.has_value()) {
        if constexpr (std::is_same_v<std::shared_ptr<std::string>, decltype(t)>) {
          // if constexpr (std::is_same_v<std::remove_reference_t<std::remove_cv_t<decltype(value.value())>>,
          // decltype(t)>) { // bad
          LOG(INFO) << (*value.value().get());
        } else if constexpr (std::is_same_v<std::shared_ptr<std::vector<bool>>, decltype(t)> ||
                             std::is_same_v<std::shared_ptr<std::vector<int32_t>>, decltype(t)> ||
                             std::is_same_v<std::shared_ptr<std::vector<int64_t>>, decltype(t)> ||
                             std::is_same_v<std::shared_ptr<std::vector<float>>, decltype(t)> ||
                             std::is_same_v<std::shared_ptr<std::vector<double>>, decltype(t)> ||
                             std::is_same_v<std::shared_ptr<std::vector<std::string>>, decltype(t)>) {
          auto lambda_show_function = [](const std::string &name, auto v) {
            LOG(INFO) << name;
            if (v) {
              for (const auto &elem : *v) {
                LOG(INFO) << elem << ", ";
              }
            }
            LOG(INFO) << "\n";
          };

          auto lambda_get_name_function = [](auto t) {
            return abi::__cxa_demangle(typeid(decltype(t)).name(), nullptr, nullptr, nullptr);
          };

          lambda_show_function(lambda_get_name_function(t), value.value());

        } else {
          LOG(INFO) << value.value();
        }

      } else {
        LOG(INFO) << "nullptr";
      }
    }
  };

  // bool
  {
    std::unique_ptr<std::vector<expr::Operand>> operand_ptr = std::make_unique<std::vector<expr::Operand>>();
    BaseSchema::Type type = BaseSchema::Type::kBool;
    std::any column;

    column = std::optional<bool>(std::nullopt);
    ok = RelExprHelper::TransToOperand(type, column, operand_ptr);
    EXPECT_EQ(ok.error_code(), pb::error::OK);

    column = std::optional<bool>(false);
    ok = RelExprHelper::TransToOperand(type, column, operand_ptr);
    EXPECT_EQ(ok.error_code(), pb::error::OK);

    call_trans_from_operand_lambda(type, operand_ptr, bool());
  }

  // int
  {
    std::unique_ptr<std::vector<expr::Operand>> operand_ptr = std::make_unique<std::vector<expr::Operand>>();
    BaseSchema::Type type = BaseSchema::Type::kInteger;
    std::any column;

    column = std::optional<int>(std::nullopt);
    ok = RelExprHelper::TransToOperand(type, column, operand_ptr);
    EXPECT_EQ(ok.error_code(), pb::error::OK);

    column = std::optional<int>(1);
    ok = RelExprHelper::TransToOperand(type, column, operand_ptr);
    EXPECT_EQ(ok.error_code(), pb::error::OK);

    call_trans_from_operand_lambda(type, operand_ptr, int());
  }

  // float
  {
    std::unique_ptr<std::vector<expr::Operand>> operand_ptr = std::make_unique<std::vector<expr::Operand>>();
    BaseSchema::Type type = BaseSchema::Type::kFloat;
    std::any column;

    column = std::optional<float>(std::nullopt);
    ok = RelExprHelper::TransToOperand(type, column, operand_ptr);
    EXPECT_EQ(ok.error_code(), pb::error::OK);

    column = std::optional<float>(1.23f);
    ok = RelExprHelper::TransToOperand(type, column, operand_ptr);
    EXPECT_EQ(ok.error_code(), pb::error::OK);

    call_trans_from_operand_lambda(type, operand_ptr, float());
  }

  // int64
  {
    std::unique_ptr<std::vector<expr::Operand>> operand_ptr = std::make_unique<std::vector<expr::Operand>>();
    BaseSchema::Type type = BaseSchema::Type::kLong;
    std::any column;

    column = std::optional<int64_t>(std::nullopt);
    ok = RelExprHelper::TransToOperand(type, column, operand_ptr);
    EXPECT_EQ(ok.error_code(), pb::error::OK);

    column = std::optional<int64_t>(1);
    ok = RelExprHelper::TransToOperand(type, column, operand_ptr);
    EXPECT_EQ(ok.error_code(), pb::error::OK);
    call_trans_from_operand_lambda(type, operand_ptr, int64_t());
  }

  // double
  {
    std::unique_ptr<std::vector<expr::Operand>> operand_ptr = std::make_unique<std::vector<expr::Operand>>();
    BaseSchema::Type type = BaseSchema::Type::kDouble;
    std::any column;

    column = std::optional<double>(std::nullopt);
    ok = RelExprHelper::TransToOperand(type, column, operand_ptr);
    EXPECT_EQ(ok.error_code(), pb::error::OK);

    column = std::optional<double>(1.23);
    ok = RelExprHelper::TransToOperand(type, column, operand_ptr);
    EXPECT_EQ(ok.error_code(), pb::error::OK);
    call_trans_from_operand_lambda(type, operand_ptr, double());
  }

  //  string
  {
    std::unique_ptr<std::vector<expr::Operand>> operand_ptr = std::make_unique<std::vector<expr::Operand>>();
    BaseSchema::Type type = BaseSchema::Type::kString;
    std::any column;

    column = std::optional<std::shared_ptr<std::string>>(std::nullopt);
    ok = RelExprHelper::TransToOperand(type, column, operand_ptr);
    EXPECT_EQ(ok.error_code(), pb::error::OK);

    column = std::optional<std::shared_ptr<std::string>>(std::make_shared<std::string>("123"));
    ok = RelExprHelper::TransToOperand(type, column, operand_ptr);
    EXPECT_EQ(ok.error_code(), pb::error::OK);
    call_trans_from_operand_lambda(type, operand_ptr, std::shared_ptr<std::string>());
  }

  // bool list
  {
    std::unique_ptr<std::vector<expr::Operand>> operand_ptr = std::make_unique<std::vector<expr::Operand>>();
    BaseSchema::Type type = BaseSchema::Type::kBoolList;
    std::any column;

    column = std::optional<std::shared_ptr<std::vector<bool>>>(std::nullopt);
    ok = RelExprHelper::TransToOperand(type, column, operand_ptr);
    EXPECT_EQ(ok.error_code(), pb::error::OK);

    std::vector<bool> bool_list{false, true, false};
    column = std::optional<std::shared_ptr<std::vector<bool>>>(std::make_shared<std::vector<bool>>(bool_list));
    ok = RelExprHelper::TransToOperand(type, column, operand_ptr);
    EXPECT_EQ(ok.error_code(), pb::error::OK);

    call_trans_from_operand_lambda(type, operand_ptr, std::shared_ptr<std::vector<bool>>());
  }

  // int list
  {
    std::unique_ptr<std::vector<expr::Operand>> operand_ptr = std::make_unique<std::vector<expr::Operand>>();
    BaseSchema::Type type = BaseSchema::Type::kIntegerList;
    std::any column;

    column = std::optional<std::shared_ptr<std::vector<int32_t>>>(std::nullopt);
    ok = RelExprHelper::TransToOperand(type, column, operand_ptr);
    EXPECT_EQ(ok.error_code(), pb::error::OK);

    std::vector<int32_t> int32_list{1, 2, 3};
    column = std::optional<std::shared_ptr<std::vector<int32_t>>>(std::make_shared<std::vector<int32_t>>(int32_list));
    ok = RelExprHelper::TransToOperand(type, column, operand_ptr);
    EXPECT_EQ(ok.error_code(), pb::error::OK);

    call_trans_from_operand_lambda(type, operand_ptr, std::shared_ptr<std::vector<int>>());
  }

  // float list
  {
    std::unique_ptr<std::vector<expr::Operand>> operand_ptr = std::make_unique<std::vector<expr::Operand>>();
    BaseSchema::Type type = BaseSchema::Type::kFloatList;
    std::any column;

    column = std::optional<std::shared_ptr<std::vector<float>>>(std::nullopt);
    ok = RelExprHelper::TransToOperand(type, column, operand_ptr);
    EXPECT_EQ(ok.error_code(), pb::error::OK);

    std::vector<float> float_list{1.23f, 2.23f, 3.23f};
    column = std::optional<std::shared_ptr<std::vector<float>>>(std::make_shared<std::vector<float>>(float_list));
    ok = RelExprHelper::TransToOperand(type, column, operand_ptr);
    EXPECT_EQ(ok.error_code(), pb::error::OK);

    call_trans_from_operand_lambda(type, operand_ptr, std::shared_ptr<std::vector<float>>());
  }

  // int64 list
  {
    std::unique_ptr<std::vector<expr::Operand>> operand_ptr = std::make_unique<std::vector<expr::Operand>>();
    BaseSchema::Type type = BaseSchema::Type::kLongList;
    std::any column;

    column = std::optional<std::shared_ptr<std::vector<int64_t>>>(std::nullopt);
    ok = RelExprHelper::TransToOperand(type, column, operand_ptr);
    EXPECT_EQ(ok.error_code(), pb::error::OK);

    std::vector<int64_t> int64_list{1000, 2000, 3000};
    column = std::optional<std::shared_ptr<std::vector<int64_t>>>(std::make_shared<std::vector<int64_t>>(int64_list));
    ok = RelExprHelper::TransToOperand(type, column, operand_ptr);
    EXPECT_EQ(ok.error_code(), pb::error::OK);

    call_trans_from_operand_lambda(type, operand_ptr, std::shared_ptr<std::vector<int64_t>>());
  }

  // double list
  {
    std::unique_ptr<std::vector<expr::Operand>> operand_ptr = std::make_unique<std::vector<expr::Operand>>();
    BaseSchema::Type type = BaseSchema::Type::kDoubleList;
    std::any column;

    column = std::optional<std::shared_ptr<std::vector<double>>>(std::nullopt);
    ok = RelExprHelper::TransToOperand(type, column, operand_ptr);
    EXPECT_EQ(ok.error_code(), pb::error::OK);

    std::vector<double> double_list{10000.23f, 200000.23f, 300000.23f};
    column = std::optional<std::shared_ptr<std::vector<double>>>(std::make_shared<std::vector<double>>(double_list));
    ok = RelExprHelper::TransToOperand(type, column, operand_ptr);
    EXPECT_EQ(ok.error_code(), pb::error::OK);

    call_trans_from_operand_lambda(type, operand_ptr, std::shared_ptr<std::vector<double>>());
  }

  //  string list
  {
    std::unique_ptr<std::vector<expr::Operand>> operand_ptr = std::make_unique<std::vector<expr::Operand>>();
    BaseSchema::Type type = BaseSchema::Type::kStringList;
    std::any column;

    column = std::optional<std::shared_ptr<std::vector<std::string>>>(std::nullopt);
    ok = RelExprHelper::TransToOperand(type, column, operand_ptr);
    EXPECT_EQ(ok.error_code(), pb::error::OK);

    std::vector<std::string> string_list{"vvvvvvv", "wwwwwwww", "yyyyyyyy"};
    column = std::optional<std::shared_ptr<std::vector<std::string>>>(
        std::make_shared<std::vector<std::string>>(string_list));
    ok = RelExprHelper::TransToOperand(type, column, operand_ptr);
    EXPECT_EQ(ok.error_code(), pb::error::OK);

    call_trans_from_operand_lambda(type, operand_ptr, std::shared_ptr<std::vector<std::string>>());
  }
}

TEST_F(RelExprHelperTest, TransToOperandWrapper) {
  butil::Status ok;

  // original_record size = 0 ok ignore
  {
    std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>> original_serial_schemas;
    std::vector<int> selection_column_indexes;
    std::vector<std::any> original_record;
    std::unique_ptr<std::vector<expr::Operand>> operand_ptr;

    ok = RelExprHelper::TransToOperandWrapper(original_serial_schemas, selection_column_indexes, original_record,
                                              operand_ptr);
    EXPECT_EQ(ok.error_code(), pb::error::OK);
  }

  // empty ok
  {
    google::protobuf::RepeatedPtrField<pb::common::Schema> pb_schemas;
    std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>> original_serial_schemas =
        std::make_shared<std::vector<std::shared_ptr<BaseSchema>>>();
    std::vector<int> selection_column_indexes;
    std::vector<std::any> original_record;
    std::unique_ptr<std::vector<expr::Operand>> operand_ptr;

    ok = Utils::TransToSerialSchema(pb_schemas, &original_serial_schemas);
    EXPECT_EQ(ok.error_code(), pb::error::OK);

    ok = RelExprHelper::TransToOperandWrapper(original_serial_schemas, selection_column_indexes, original_record,
                                              operand_ptr);
    EXPECT_EQ(ok.error_code(), pb::error::OK);
  }

  //
  {
    google::protobuf::RepeatedPtrField<pb::common::Schema> pb_schemas;
    std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>> original_serial_schemas =
        std::make_shared<std::vector<std::shared_ptr<BaseSchema>>>();
    std::vector<int> selection_column_indexes;
    std::vector<std::any> original_record;
    std::unique_ptr<std::vector<expr::Operand>> operand_ptr = std::make_unique<std::vector<expr::Operand>>();

    original_serial_schemas->reserve(6);

    std::shared_ptr<DingoSchema<std::optional<bool>>> bool_schema =
        std::make_shared<DingoSchema<std::optional<bool>>>();
    bool_schema->SetIsKey(true);
    bool_schema->SetAllowNull(true);
    bool_schema->SetIndex(5);

    original_serial_schemas->emplace_back(std::move(bool_schema));

    std::shared_ptr<DingoSchema<std::optional<int32_t>>> int_schema =
        std::make_shared<DingoSchema<std::optional<int32_t>>>();
    int_schema->SetIsKey(false);
    int_schema->SetAllowNull(true);
    int_schema->SetIndex(4);
    original_serial_schemas->emplace_back(std::move(int_schema));

    std::shared_ptr<DingoSchema<std::optional<float>>> float_schema =
        std::make_shared<DingoSchema<std::optional<float>>>();
    float_schema->SetIsKey(false);
    float_schema->SetAllowNull(true);
    float_schema->SetIndex(3);
    original_serial_schemas->emplace_back(std::move(float_schema));

    std::shared_ptr<DingoSchema<std::optional<int64_t>>> long_schema =
        std::make_shared<DingoSchema<std::optional<int64_t>>>();
    long_schema->SetIsKey(false);
    long_schema->SetAllowNull(true);
    long_schema->SetIndex(2);
    original_serial_schemas->emplace_back(std::move(long_schema));

    std::shared_ptr<DingoSchema<std::optional<double>>> double_schema =
        std::make_shared<DingoSchema<std::optional<double>>>();
    double_schema->SetIsKey(true);
    double_schema->SetAllowNull(true);
    double_schema->SetIndex(1);
    original_serial_schemas->emplace_back(std::move(double_schema));

    std::shared_ptr<DingoSchema<std::optional<std::shared_ptr<std::string>>>> string_schema =
        std::make_shared<DingoSchema<std::optional<std::shared_ptr<std::string>>>>();
    string_schema->SetIsKey(true);
    string_schema->SetAllowNull(true);
    string_schema->SetIndex(0);
    original_serial_schemas->emplace_back(std::move(string_schema));

    selection_column_indexes.push_back(5);
    selection_column_indexes.push_back(4);
    selection_column_indexes.push_back(3);
    selection_column_indexes.push_back(2);
    selection_column_indexes.push_back(1);
    selection_column_indexes.push_back(0);

    original_record.emplace_back(
        std::optional<std::shared_ptr<std::string>>(std::make_shared<std::string>("vvvvvvvvvvvvvvv")));
    original_record.emplace_back(std::optional<double>(12122323.454));
    original_record.emplace_back(std::optional<int64_t>(10000000));
    original_record.emplace_back(std::optional<float>(1.23f));
    original_record.emplace_back(std::optional<int32_t>(10));
    original_record.emplace_back(std::optional<bool>(true));
    ok = RelExprHelper::TransToOperandWrapper(original_serial_schemas, selection_column_indexes, original_record,
                                              operand_ptr);
    EXPECT_EQ(ok.error_code(), pb::error::OK);

    for (const auto &operand : *operand_ptr) {
      try {
        auto v = operand.GetValue<bool>();
        LOG(INFO) << "bool : " << v;
        continue;
      } catch (const std::exception &e) {
        LOG(INFO) << "exception : " << e.what();
      }

      try {
        auto v = operand.GetValue<int>();
        LOG(INFO) << "int : " << v;
        continue;
      } catch (const std::exception &e) {
        LOG(INFO) << "exception : " << e.what();
      }

      try {
        auto v = operand.GetValue<float>();
        LOG(INFO) << "float : " << v;
        continue;
      } catch (const std::exception &e) {
        LOG(INFO) << "exception : " << e.what();
      }

      try {
        auto v = operand.GetValue<int64_t>();
        LOG(INFO) << "int64_t : " << v;
        continue;
      } catch (const std::exception &e) {
        LOG(INFO) << "exception : " << e.what();
      }

      try {
        auto v = operand.GetValue<double>();
        LOG(INFO) << "double : " << v;
        continue;
      } catch (const std::exception &e) {
        LOG(INFO) << "exception : " << e.what();
      }

      try {
        auto v = operand.GetValue<dingodb::expr::String>();
        LOG(INFO) << "std::shared_ptr<std::string> : " << *v;
        continue;
      } catch (const std::exception &e) {
        LOG(INFO) << "exception : " << e.what();
      }

      try {
        auto v = operand.GetValue<std::monostate>();
        LOG(INFO) << "std::monostate : "
                  << "nullptr";
        continue;
      } catch (const std::exception &e) {
        LOG(INFO) << "exception : " << e.what();
      }
    }
  }
}

TEST_F(RelExprHelperTest, TransFromOperandWrapper) {
  butil::Status ok;

  // operand_ptr size = 0 ok ignore
  {
    std::unique_ptr<std::vector<expr::Operand>> operand_ptr = std::make_unique<std::vector<expr::Operand>>();
    std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>> result_serial_schemas;
    std::vector<int> result_column_indexes;
    std::vector<std::any> result_record;

    ok = RelExprHelper::TransFromOperandWrapper(operand_ptr, result_serial_schemas, result_column_indexes,
                                                result_record);
  }

  // order
  {
    std::unique_ptr<std::vector<expr::Operand>> operand_ptr = std::make_unique<std::vector<expr::Operand>>();
    std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>> result_serial_schemas =
        std::make_shared<std::vector<std::shared_ptr<BaseSchema>>>();
    std::vector<int> result_column_indexes;
    std::vector<std::any> result_record;

    result_serial_schemas->reserve(6);

    std::shared_ptr<DingoSchema<std::optional<bool>>> bool_schema =
        std::make_shared<DingoSchema<std::optional<bool>>>();
    bool_schema->SetIsKey(true);
    bool_schema->SetAllowNull(true);
    bool_schema->SetIndex(0);

    result_serial_schemas->emplace_back(std::move(bool_schema));

    std::shared_ptr<DingoSchema<std::optional<int32_t>>> int_schema =
        std::make_shared<DingoSchema<std::optional<int32_t>>>();
    int_schema->SetIsKey(true);
    int_schema->SetAllowNull(true);
    int_schema->SetIndex(1);
    result_serial_schemas->emplace_back(std::move(int_schema));

    std::shared_ptr<DingoSchema<std::optional<float>>> float_schema =
        std::make_shared<DingoSchema<std::optional<float>>>();
    float_schema->SetIsKey(false);
    float_schema->SetAllowNull(true);
    float_schema->SetIndex(2);
    result_serial_schemas->emplace_back(std::move(float_schema));

    std::shared_ptr<DingoSchema<std::optional<int64_t>>> long_schema =
        std::make_shared<DingoSchema<std::optional<int64_t>>>();
    long_schema->SetIsKey(false);
    long_schema->SetAllowNull(true);
    long_schema->SetIndex(3);
    result_serial_schemas->emplace_back(std::move(long_schema));

    std::shared_ptr<DingoSchema<std::optional<double>>> double_schema =
        std::make_shared<DingoSchema<std::optional<double>>>();
    double_schema->SetIsKey(false);
    double_schema->SetAllowNull(true);
    double_schema->SetIndex(4);
    result_serial_schemas->emplace_back(std::move(double_schema));

    std::shared_ptr<DingoSchema<std::optional<std::shared_ptr<std::string>>>> string_schema =
        std::make_shared<DingoSchema<std::optional<std::shared_ptr<std::string>>>>();
    string_schema->SetIsKey(false);
    string_schema->SetAllowNull(true);
    string_schema->SetIndex(5);
    result_serial_schemas->emplace_back(std::move(string_schema));

    // bool
    {
      BaseSchema::Type type = BaseSchema::Type::kBool;
      std::any column;

      column = std::optional<bool>(false);
      ok = RelExprHelper::TransToOperand(type, column, operand_ptr);
      EXPECT_EQ(ok.error_code(), pb::error::OK);
    }

    // int
    {
      BaseSchema::Type type = BaseSchema::Type::kInteger;
      std::any column;

      column = std::optional<int>(1);
      ok = RelExprHelper::TransToOperand(type, column, operand_ptr);
      EXPECT_EQ(ok.error_code(), pb::error::OK);
    }

    // float
    {
      BaseSchema::Type type = BaseSchema::Type::kFloat;
      std::any column;

      column = std::optional<float>(1.23f);
      ok = RelExprHelper::TransToOperand(type, column, operand_ptr);
      EXPECT_EQ(ok.error_code(), pb::error::OK);
    }

    // int64
    {
      BaseSchema::Type type = BaseSchema::Type::kLong;
      std::any column;

      column = std::optional<int64_t>(1);
      ok = RelExprHelper::TransToOperand(type, column, operand_ptr);
      EXPECT_EQ(ok.error_code(), pb::error::OK);
    }

    // double
    {
      BaseSchema::Type type = BaseSchema::Type::kDouble;
      std::any column;

      column = std::optional<double>(1.23);
      ok = RelExprHelper::TransToOperand(type, column, operand_ptr);
      EXPECT_EQ(ok.error_code(), pb::error::OK);
    }

    //  string
    {
      BaseSchema::Type type = BaseSchema::Type::kString;
      std::any column;

      column = std::optional<std::shared_ptr<std::string>>(std::make_shared<std::string>("vvvvvvvvvvvvvvv"));
      ok = RelExprHelper::TransToOperand(type, column, operand_ptr);
      EXPECT_EQ(ok.error_code(), pb::error::OK);
    }

    result_column_indexes.push_back(0);
    result_column_indexes.push_back(1);
    result_column_indexes.push_back(2);
    result_column_indexes.push_back(3);
    result_column_indexes.push_back(4);
    result_column_indexes.push_back(5);

    ok = RelExprHelper::TransFromOperandWrapper(operand_ptr, result_serial_schemas, result_column_indexes,
                                                result_record);

    for (const auto &record : result_record) {
      auto lambda_function = [&record](auto t, bool &is_ok) {
        try {
          if (record.type() != typeid(std::optional<decltype(t)>)) {
            return;
          }

          const auto &value = std::any_cast<std::optional<decltype(t)>>(&record);
          LOG(INFO) << abi::__cxa_demangle(typeid(t).name(), nullptr, nullptr, nullptr) << " : ";

          if (value->has_value()) {
            if constexpr (std::is_same_v<std::shared_ptr<std::string>, decltype(t)>) {
              // LOG(INFO) << *(value->value().get());
              LOG(INFO) << *(value->value());
            } else {
              LOG(INFO) << value->value();
            }
            is_ok = true;
          } else {
            LOG(INFO) << "nullptr";
          }
        } catch (std::exception &e) {
          LOG(INFO) << "exception : std::get: wrong index for variant"
                    << "\n";
        }
      };
      bool is_ok = false;
      if (!is_ok) {
        lambda_function(bool(), is_ok);
      }

      if (!is_ok) {
        lambda_function(int32_t(), is_ok);
      }
      if (!is_ok) {
        lambda_function(float(), is_ok);
      }
      if (!is_ok) {
        lambda_function(int64_t(), is_ok);
      }
      if (!is_ok) {
        lambda_function(double(), is_ok);
      }
      if (!is_ok) {
        lambda_function(std::shared_ptr<std::string>(), is_ok);
      }
    }
  }

  // disorder
  {
    std::unique_ptr<std::vector<expr::Operand>> operand_ptr = std::make_unique<std::vector<expr::Operand>>();
    std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>> result_serial_schemas =
        std::make_shared<std::vector<std::shared_ptr<BaseSchema>>>();
    std::vector<int> result_column_indexes;
    std::vector<std::any> result_record;

    result_serial_schemas->reserve(6);

    std::shared_ptr<DingoSchema<std::optional<bool>>> bool_schema =
        std::make_shared<DingoSchema<std::optional<bool>>>();
    bool_schema->SetIsKey(true);
    bool_schema->SetAllowNull(true);
    bool_schema->SetIndex(5);

    result_serial_schemas->emplace_back(std::move(bool_schema));

    std::shared_ptr<DingoSchema<std::optional<int32_t>>> int_schema =
        std::make_shared<DingoSchema<std::optional<int32_t>>>();
    int_schema->SetIsKey(true);
    int_schema->SetAllowNull(true);
    int_schema->SetIndex(4);
    result_serial_schemas->emplace_back(std::move(int_schema));

    std::shared_ptr<DingoSchema<std::optional<float>>> float_schema =
        std::make_shared<DingoSchema<std::optional<float>>>();
    float_schema->SetIsKey(false);
    float_schema->SetAllowNull(true);
    float_schema->SetIndex(3);
    result_serial_schemas->emplace_back(std::move(float_schema));

    std::shared_ptr<DingoSchema<std::optional<int64_t>>> long_schema =
        std::make_shared<DingoSchema<std::optional<int64_t>>>();
    long_schema->SetIsKey(false);
    long_schema->SetAllowNull(true);
    long_schema->SetIndex(2);
    result_serial_schemas->emplace_back(std::move(long_schema));

    std::shared_ptr<DingoSchema<std::optional<double>>> double_schema =
        std::make_shared<DingoSchema<std::optional<double>>>();
    double_schema->SetIsKey(false);
    double_schema->SetAllowNull(true);
    double_schema->SetIndex(1);
    result_serial_schemas->emplace_back(std::move(double_schema));

    std::shared_ptr<DingoSchema<std::optional<std::shared_ptr<std::string>>>> string_schema =
        std::make_shared<DingoSchema<std::optional<std::shared_ptr<std::string>>>>();
    string_schema->SetIsKey(false);
    string_schema->SetAllowNull(true);
    string_schema->SetIndex(0);
    result_serial_schemas->emplace_back(std::move(string_schema));

    //  string
    {
      BaseSchema::Type type = BaseSchema::Type::kString;
      std::any column;

      column = std::optional<std::shared_ptr<std::string>>(std::make_shared<std::string>("vvvvvvvvvvvvvvv"));
      ok = RelExprHelper::TransToOperand(type, column, operand_ptr);
      EXPECT_EQ(ok.error_code(), pb::error::OK);
    }

    // double
    {
      BaseSchema::Type type = BaseSchema::Type::kDouble;
      std::any column;

      column = std::optional<double>(1.23);
      ok = RelExprHelper::TransToOperand(type, column, operand_ptr);
      EXPECT_EQ(ok.error_code(), pb::error::OK);
    }

    // int64
    {
      BaseSchema::Type type = BaseSchema::Type::kLong;
      std::any column;

      column = std::optional<int64_t>(1);
      ok = RelExprHelper::TransToOperand(type, column, operand_ptr);
      EXPECT_EQ(ok.error_code(), pb::error::OK);
    }

    // float
    {
      BaseSchema::Type type = BaseSchema::Type::kFloat;
      std::any column;

      column = std::optional<float>(1.23f);
      ok = RelExprHelper::TransToOperand(type, column, operand_ptr);
      EXPECT_EQ(ok.error_code(), pb::error::OK);
    }

    // int
    {
      BaseSchema::Type type = BaseSchema::Type::kInteger;
      std::any column;

      column = std::optional<int>(1);
      ok = RelExprHelper::TransToOperand(type, column, operand_ptr);
      EXPECT_EQ(ok.error_code(), pb::error::OK);
    }

    // bool
    {
      BaseSchema::Type type = BaseSchema::Type::kBool;
      std::any column;

      column = std::optional<bool>(false);
      ok = RelExprHelper::TransToOperand(type, column, operand_ptr);
      EXPECT_EQ(ok.error_code(), pb::error::OK);
    }

    result_column_indexes.push_back(5);
    result_column_indexes.push_back(4);
    result_column_indexes.push_back(3);
    result_column_indexes.push_back(2);
    result_column_indexes.push_back(1);
    result_column_indexes.push_back(0);

    ok = RelExprHelper::TransFromOperandWrapper(operand_ptr, result_serial_schemas, result_column_indexes,
                                                result_record);

    for (const auto &record : result_record) {
      auto lambda_function = [&record](auto t, bool &is_ok) {
        try {
          if (record.type() != typeid(std::optional<decltype(t)>)) {
            return;
          }

          const auto &value = std::any_cast<std::optional<decltype(t)>>(&record);
          LOG(INFO) << abi::__cxa_demangle(typeid(t).name(), nullptr, nullptr, nullptr) << " : ";

          if (value->has_value()) {
            if constexpr (std::is_same_v<std::shared_ptr<std::string>, decltype(t)>) {
              // LOG(INFO) << *(value->value().get());
              LOG(INFO) << *(value->value());
            } else {
              LOG(INFO) << value->value();
            }
            is_ok = true;
          } else {
            LOG(INFO) << "nullptr";
          }
        } catch (std::exception &e) {
          LOG(INFO) << "exception : std::get: wrong index for variant"
                    << "\n";
        }
      };
      bool is_ok = false;
      if (!is_ok) {
        lambda_function(bool(), is_ok);
      }

      if (!is_ok) {
        lambda_function(int32_t(), is_ok);
      }
      if (!is_ok) {
        lambda_function(float(), is_ok);
      }
      if (!is_ok) {
        lambda_function(int64_t(), is_ok);
      }
      if (!is_ok) {
        lambda_function(double(), is_ok);
      }
      if (!is_ok) {
        lambda_function(std::shared_ptr<std::string>(), is_ok);
      }
    }
  }
}

}  // namespace dingodb
